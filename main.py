# LocalRPA Orchestrator
# A lightweight local Python runtime that orchestrates automation jobs from email and data sources.
# It delegates UI execution to an external RPA tool (e.g. UiPath / Power Automate) via a file-based IPC ("handover").
#
# Backend (this script):
# - reads incoming jobs (email / data)
# - validates and decides actions
# - writes handover state for the RPA
# - verifies results and logs to SQLite
#
# Front-end RPA:
# - performs clicks and keyboard actions in external systems
# - reads/writes handover file to sync state
#
# Design goals:
# - run locally on a single machine (no servers or cloud)
# - no additional backend licensing required
# - simple, inspectable, and fail-safe (cold start, safestop)

# im working on a rpa orchestrator project, something small and simple for us getting started with RPA in a company. 
# it's all made in python because I find it easier to work with, but you still need a 
#  real RPA (e.g. Power Automate / UiPath Studio) for the screen-activity. Am I re-inventing the wheel? Or do you
# just do all logic in the RPA? I found RobotFramework but it's not the same.
# https://github.com/eliascccc/LocalRPA-Orchestrator/
# 


import tkinter as tk
import time, threading, traceback, os, tempfile, sys, platform, subprocess, signal, atexit, sqlite3, datetime, shutil, re, json
from openpyxl import Workbook, load_workbook #type: ignore
from typing import Never, Literal
from pathlib import Path
from email.parser import BytesParser
from email.utils import parseaddr
from dataclasses import dataclass
from email import policy



'''
job_states:
    "REJECTED",        # rejected before execution (user issue)
    "QUEUED",          # job accepted and queued to front-end robot
    "RUNNING",         # front-end robot executing
    "VERIFYING",       # verifying front-end result with SQL if possible
    "DONE",            # success
    "FAIL",          # failed or robot/system error
'''

@dataclass
class JobDecision:
    action: Literal["DELETE_ONLY", "REPLY_AND_DELETE", "QUEUE_RPA_JOB", "SKIP", "MOVE_BACK_TO_INBOX", "CRASH"]
    job_type: str | None = None
    reply_subject: str | None = None
    reply_body: str | None = None
    job_status: Literal["REJECTED", "QUEUED", "RUNNING", "VERIFYING", "DONE", "FAIL"]| None = None
    error_code: str | None = None
    error_message: str | None = None
    handover_payload: dict | None = None
    ui_log_message: str | None = None
    system_log_message: str | None = None
    send_lifesign_notice: bool = False
    start_recording: bool = False
    crash_reason: str | None = None


@dataclass
class MailJobCandidate:
    message_id: str
    sender_email: str
    sender_name: str
    subject: str
    body: str
    headers: dict[str, str]
    message_ref: Path  # in dev: Path     # in Outlook : message id 
    job_source_type: Literal["personal_inbox", "shared_inbox"]


@dataclass
class ScheduledJobCandidate:
    order_number: int
    order_qty: int
    material_available: int
    job_source_type: Literal["erp_query"] | None=None


@dataclass
class PollResult:
    handled_anything: bool
    handover_data: dict | None = None


# for fetching emails (rewire to eg. Outlook).
class FolderMailBackend:
    def __init__(self, log_system, pipeline_root) -> None:
        self.log_system = log_system
        self.pipeline_root = Path(pipeline_root) # change to folder in e.g. outlook
        self.inbox_dir = self.pipeline_root / "inbox"
        self.processing_dir = self.pipeline_root / "processing"

        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.processing_dir.mkdir(parents=True, exist_ok=True)


    def fetch_next_from_inbox(self) -> Path | None:
        email_files = sorted(self.inbox_dir.glob("*.eml"))
        if not email_files:
            return None

        self.log_system(f"found personal inbox email: {email_files[0]}")
        return email_files[0]

    def fetch_all_from_inbox(self) -> list | None:
        email_files = sorted(self.inbox_dir.glob("*.eml"))
        if not email_files:
            return None
        
        self.log_system(f"found one or many shared inbox email: {email_files[0]}")

        return email_files


    def claim_to_processing(self, mail: MailJobCandidate) -> MailJobCandidate:
        target_path = self.processing_dir / mail.message_ref.name #.name only the filenamne
        shutil.move(str(mail.message_ref), str(target_path))
        
        self.log_system(f"moved {mail.message_ref} to {target_path}")
        mail.message_ref = target_path

        return mail
        

    def parse_processing_mail(self, processing_path: Path) -> MailJobCandidate:
        with open(processing_path, "rb") as f:
            msg = BytesParser(policy=policy.default).parse(f)

        from_name, from_address = parseaddr(msg.get("From", ""))
        subject = msg.get("Subject", "").strip()

        message_id = msg.get("Message-ID", "").strip()
        if not message_id:
            message_id = processing_path.stem

        headers = {k: str(v) for k, v in msg.items()}

        if msg.is_multipart():
            body_parts = []
            for part in msg.walk():
                if part.get_content_type() == "text/plain" and not part.get_filename():
                    try:
                        body_parts.append(part.get_content())
                    except Exception:
                        pass
            body = "\n".join(body_parts).strip()
        else:
            try:
                body = msg.get_content().strip()
            except Exception:
                body = ""

     
        return MailJobCandidate(
            message_id=message_id,
            sender_email=from_address.strip().lower(),
            sender_name=from_name.strip(),
            subject=subject,
            body=body,
            headers=headers,
            message_ref=processing_path,
            job_source_type="personal_inbox" if "personal" in str(processing_path) else "shared_inbox"  ##STUB
        )

    def reply_and_delete(self, mail: MailJobCandidate, subject: str, body: str, job_id: int | None = None) -> None:
        self.send_reply(mail, subject, body, job_id)
        self.delete_from_processing(mail, job_id)

    def send_reply(self, mail: MailJobCandidate, subject: str, body: str, job_id: int | None = None) -> None:
        # DEV STUB

        reply_to = mail.sender_email
        reply_subject = subject
        reply_body = body

        reply_message = f"reply stub to={reply_to}. subject={reply_subject}'. body={reply_body}"
        self.log_system(reply_message[:120], job_id)
        
        print(f"\n*** email reply stub ************\nto={reply_to} \nsubject={reply_subject!r} \nbody='{reply_body} \n********************************\n")


    def delete_from_processing(self, mail: MailJobCandidate, job_id: int | None = None) -> None:
        if not isinstance(mail.message_ref, Path):
            raise ValueError("delete_from_processing() expected Path message_ref in dev mode")

        self.log_system(f"removing: {mail.message_ref}", job_id)
        os.remove(mail.message_ref)

    def move_back_to_inbox(self, mail: MailJobCandidate) -> None:
        #stub
        pass


# for query (rewire to eg. real ERP).
class ExcelErpBackend:

    def select_all_from_erp(self, path="Fake_ERP_table.xlsx") -> list[dict]:
        # replace with result from a no-extra-garbage query

        self.ensure_fake_erp_exists(path)

        wb = load_workbook(path)
        ws = wb.active

        assert ws is not None

        all_rows=[]

        for row in ws.iter_rows(min_row=2):  # skip header
            
            order_number = row[0].value
            order_qty = row[1].value
            material_available = row[2].value

            all_rows.append({
                    "order_number": order_number,
                    "order_qty": order_qty,
                    "material_available": material_available,
                })
            
        wb.close()
        return all_rows
    
    
    def parse_row(self, row) -> ScheduledJobCandidate:
              
        order_number = row.get("order_number")
        order_qty = row.get("order_qty")
        material_available = row.get("material_available")



        if not isinstance(order_number, str): #normally str from erp
            raise ValueError(f"invalid order_number: {order_number!r}")
        if not isinstance(order_qty, int):
            raise ValueError(f"invalid order_qty: {order_qty!r}")
        if not isinstance(material_available, int):
            raise ValueError(f"invalid material_available: {material_available!r}")

        return ScheduledJobCandidate(
            order_number=int(order_number),
            order_qty=order_qty,
            material_available=material_available,
        )

    
    def ensure_fake_erp_exists(self, path="Fake_ERP_table.xlsx") -> None:
        ''' a table in ERP '''
        if os.path.exists(path):
            return

        wb = Workbook()
        ws = wb.active
        assert ws is not None

        # headers
        ws["A1"] = "order_number"
        ws["B1"] = "order_qty"
        ws["C1"] = "material_available"

        wb.save(path)
        wb.close()

    
    def get_order_qty(self, order_number, path="Fake_ERP_table.xlsx") -> int | None:
        self.ensure_fake_erp_exists(path)

        wb = load_workbook(path)
        ws = wb.active
        assert ws is not None

        for row in ws.iter_rows(min_row=2):
            cell_order_number = row[0].value

            if str(cell_order_number) == str(order_number):
                value = row[1].value  # order_qty    #stype: ignore

                if isinstance(value, int):
                    wb.close()
                    return int(value)
                
                else: 
                    raise ValueError(f"order_qty: {value} is not INT")
        
        wb.close()
        return None  # not found


#for email-pipeline
class MailFlow:
    def __init__(self, log_system, log_ui_and_system, friends_repo, is_within_operating_hours, network_service, job_handlers, pre_handover_executor) -> None:
        self.log_system = log_system
        self.log_ui_and_system = log_ui_and_system
        self.friends_repo = friends_repo
        self.is_within_operating_hours = is_within_operating_hours
        self.network_service = network_service
        self.job_handlers = job_handlers
        self.pre_handover_executor = pre_handover_executor
        self.mail_backend_shared = FolderMailBackend(self.log_system, pipeline_root="shared_inbox",  )
        self.mail_backend_personal = FolderMailBackend(self.log_system, pipeline_root="personal_inbox",  )

    def poll_once(self) -> PollResult:
        ''' a candidate is an email from personal inbox OR an 'in scope'-email from shared inbox '''

        candidate = self.claim_next_mail_candidate() #claimed and parsed from all mail-sources
        if not candidate:
            return PollResult(handled_anything=False, handover_data=None)
        

        elif candidate.job_source_type == "personal_inbox":
            if self.friends_repo.reload_if_modified():
                self.log_ui_and_system("friends.xlsx reloaded", blank_line_before=True)

            self.log_ui_and_system(f"email from {candidate.sender_email}", blank_line_before=True)
            decision = self.decide_own_inbox_email(candidate)


        elif candidate.job_source_type == "shared_inbox":
            decision = self.decide_shared_inbox_email(candidate)

        
        else:
            raise RuntimeError(f"unknown source type for candidate: {candidate.job_source_type}")
            
        
        mail_backend = self.get_mail_backend_for_candidate(candidate)
        handover_data = self.pre_handover_executor.execute_decision(candidate, decision, mail_backend)
        return PollResult(handled_anything=True, handover_data=handover_data)


    def claim_next_mail_candidate(self) -> MailJobCandidate | None:

        # personal inbox priority (parse, always claim)
        inbox_path = self.mail_backend_personal.fetch_next_from_inbox()
       
        if inbox_path:
            mail = self.mail_backend_personal.parse_processing_mail(inbox_path)
            del inbox_path
            
            mail = self.mail_backend_personal.claim_to_processing(mail)
            self.log_system(f"{mail.job_source_type} produced mail {mail.message_id}")
            return mail

        
        # shared inbox (parse, maybe claim)
        list_of_inbox_path = self.mail_backend_shared.fetch_all_from_inbox()
        if not list_of_inbox_path:
            return None
        
        for inbox_path in list_of_inbox_path:

            mail = self.mail_backend_shared.parse_processing_mail(inbox_path)
            del inbox_path

            if not self.is_shared_inbox_email_in_scope(mail):
                continue
            
            mail = self.mail_backend_shared.claim_to_processing(mail)
            self.log_system(f"{mail.job_source_type} produced mail {mail.message_id}")

            return mail


        return None


    def is_shared_inbox_email_in_scope(self,mail):
        #placeholder for checks, e.g. invoices from CompanyX that starts with '20....' 
        self.log_system(f"placeholder scope check for {mail}")
        return True

    
    def classify_personal_inbox_email(self, mail: MailJobCandidate) -> str:
        subject = mail.subject.strip().lower()

        if subject.startswith("ping"):
            return "ping"
        
        elif "job1" in subject:
            return "job1"
        
        elif "job2" in subject:
            return "job2"

        return "unknown"


    def classify_shared_inbox_email(self):
        #stub
        pass
 

    def decide_own_inbox_email(self, mail: MailJobCandidate) -> JobDecision:
        job_type = None

        try:
            if not self.friends_repo.is_allowed_sender(mail.sender_email):
                return JobDecision(
                    action="DELETE_ONLY",
                    ui_log_message="--> rejected (not in friends.xlsx)",
                )

            if not self.is_within_operating_hours():
                return JobDecision(
                    action="REPLY_AND_DELETE",
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body="Email received outside working hours 05-23.",
                    job_status="REJECTED",
                    error_code="OUTSIDE_WORKING_HOURS",
                    ui_log_message="--> rejected (outside working hours)",
                )

            job_type = self.classify_personal_inbox_email(mail)

            if job_type == "unknown":
                return JobDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body="Could not identify a job type.",
                    job_status="REJECTED",
                    error_code="UNKNOWN_JOB",
                    ui_log_message="--> rejected (unable to identify job type)",
                )

            if not self.friends_repo.has_job_access(mail.sender_email, job_type):
                return JobDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body=f"No access to {job_type}. Check with administrator for access.",
                    job_status="REJECTED",
                    error_code="NO_ACCESS",
                    ui_log_message=f"--> rejected (no access to {job_type})",
                )



            if not self.network_service.has_network_access():
                return JobDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body="No network connection. Your email was removed.",
                    job_status="REJECTED",
                    error_code="NO_NETWORK",
                    ui_log_message="--> rejected (no network connection)",
                )

            handler = self.job_handlers.get(job_type)
            if handler is None:
                return JobDecision(
                    action="CRASH",
                    job_type=job_type,
                    crash_reason=f"No handler registered for job_type={job_type}",
                )

            ok, payload_or_error = handler.precheck_and_build_payload(mail)
            if not ok:
                error = str(payload_or_error)
                return JobDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body=error, #error message from precheck...()
                    job_status="REJECTED",
                    error_code="INVALID_INPUT",
                    error_message=error,
                    ui_log_message=f"--> rejected (invalid input for {job_type})",
                )
            

            if job_type == "ping":
                handler.play_sound()
                return JobDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"DONE re: {mail.subject}",
                    reply_body="PONG (robot online).",
                    job_status="DONE",
                    ui_log_message="--> done (ping)",
                )
            
            payload = payload_or_error

            return JobDecision(
                action="QUEUE_RPA_JOB",
                job_type=job_type,
                job_status="QUEUED",
                system_log_message=f"accepted ({job_type})",
                send_lifesign_notice=True,
                start_recording=True,
                handover_payload={
                    "job_type": job_type,
                    "message_id": mail.message_id, # or 'path'
                    "sender_email": mail.sender_email,
                    "sender_name": mail.sender_name,
                    "subject": mail.subject,
                    "body": mail.body,
                    "job_source_type": mail.job_source_type,
                    "message_ref": str(mail.message_ref) if mail.message_ref is not None else "",
                    **payload,
                },
            )

        except Exception as err:
            return JobDecision(
                action="CRASH",
                job_type=job_type,
                crash_reason=str(err),
            )
    
 
    def decide_shared_inbox_email(self, mail: MailJobCandidate) -> JobDecision:
        #stub
        return JobDecision(
                    action="MOVE_BACK_TO_INBOX",
                    system_log_message=f"No logic yet, move back this email to inbox from proccessing-folder: {mail.sender_email}" #only in DEV
                )



    def get_mail_backend_for_candidate(self, mail: MailJobCandidate) -> FolderMailBackend:
        if mail.job_source_type == "personal_inbox":
            return self.mail_backend_personal
        if mail.job_source_type == "shared_inbox":
            return self.mail_backend_shared
        raise ValueError(f"unknown job_source_type={mail.job_source_type}")


# for scheduledjobs-pipeline
class ScheduledFlow:
    ''' scheduled jobs pipeline '''
    def __init__(self, log_system, log_ui_and_system, audit_repo, job_handlers, in_dev_mode, pre_handover_executor) -> None:
        self.in_dev_mode = in_dev_mode
        self.log_system = log_system
        self.log_ui_and_system = log_ui_and_system
        self.audit_repo = audit_repo
        self.job_handlers = job_handlers
        self.pre_handover_executor = pre_handover_executor
        self.excel_erp_backend = ExcelErpBackend()

        self.poll_interval = 1 if self.in_dev_mode else 600   # 600 = 10 min
        self.next_check_time = 0


    def poll_once(self) -> PollResult:
        #candidate can be a row from a query 

        now = time.time()

        if now > self.next_check_time:
            self.next_check_time = now + self.poll_interval

            candidate = self.fetch_next_scheduled_candidate()
            if not candidate:
                return PollResult(handled_anything=False, handover_data=None)


            self.log_ui_and_system(f"scheduled job detected: {candidate.order_number}", blank_line_before=True)
            decision = self.decide_candidate(candidate)

            
            handover_data = self.pre_handover_executor.execute_decision(candidate, decision)
            return PollResult(handled_anything=True, handover_data=handover_data)
        
        return PollResult(handled_anything=False, handover_data=None)


    def fetch_next_scheduled_candidate(self) -> ScheduledJobCandidate | None:

        # job 3
        all_selected_rows_query3 = self.excel_erp_backend.select_all_from_erp()
        
        if not all_selected_rows_query3:
            return None
    
        for row_candidate_raw in all_selected_rows_query3:
            row_candidate = self.excel_erp_backend.parse_row(row_candidate_raw)

            # avoid bad loops by not working the same row twice a day
            if self.audit_repo.has_been_processed_today(row_candidate.order_number):
                continue

            row_candidate.job_source_type="erp_query"
            self.log_system(f"{row_candidate.job_source_type} produced order_number {row_candidate.order_number}")
            return row_candidate
        
        # job 4
        # stub
        
        return None


    def decide_candidate(self, candidate_row: ScheduledJobCandidate) -> JobDecision:
        self.log_system("running")

        job_type = None

        try:

                        #placeholder evaluation logic
                        # eg. below:


            job_type = self.classify_candidate(candidate_row)
            handler = self.job_handlers.get(job_type)

            if candidate_row.material_available < 100:
                return JobDecision(
                    action="SKIP",
                    job_type=job_type,
                    job_status="REJECTED",
                    error_message="too few material available, manual check required",
                    ui_log_message=f"--> rejected (manual check required for {job_type})",
                )
            

            if handler is None:
                return JobDecision(
                    action="CRASH",
                    job_type=job_type,
                    crash_reason=f"No handler registered for job_type={job_type}",
                )

            ok, payload_or_error = handler.precheck_and_build_payload(candidate_row)

            if not ok:
                error = str(payload_or_error)
                return JobDecision(
                    action="SKIP",  
                    job_type=job_type,
                    job_status="REJECTED",
                    error_code="INVALID_INPUT",
                    error_message=error,
                    ui_log_message=f"--> rejected (invalid input for {job_type})",
                )

            payload = payload_or_error
            
            
            return JobDecision(
                action="QUEUE_RPA_JOB",
                job_type=job_type,
                job_status="QUEUED",
                system_log_message=f"accepted ({job_type})",
                start_recording=True,
                handover_payload={
                    "job_type": job_type,
                    "order_number": str(candidate_row.order_number),
                    "order_qty": str(candidate_row.order_qty),
                    "material_available": str(candidate_row.material_available),
                    "expected_action": "sync_qty_to_material_available",
                    "job_source_type": "erp_query",
                    **payload,
                },
                )

        except Exception as err:
            return JobDecision(
                action="CRASH",
                job_type=job_type,
                crash_reason=str(err),
            )
    

    def classify_candidate(self, row: ScheduledJobCandidate) -> str:
        #stub
        self.log_system("running. STUB")
        del row
        return "job3"


# for decision-making on the found job
class PreHandoverExecutor:
    def __init__(self, log_system, log_ui_and_system, update_ui_status, refresh_jobs_done_today_display, ui_dot_tk_set_show_recording_overlay, generate_job_id, recording_service, audit_repo, safestop_controller, in_dev_mode) -> None:
        self.in_dev_mode = in_dev_mode
        self.log_system = log_system
        self.log_ui_and_system = log_ui_and_system
        self.recording_service = recording_service
        self.generate_job_id = generate_job_id
        self.audit_repo = audit_repo
        self.update_ui_status = update_ui_status
        self.refresh_jobs_done_today_display = refresh_jobs_done_today_display
        self.ui_dot_tk_set_show_recording_overlay = ui_dot_tk_set_show_recording_overlay
        self.safestop_controller = safestop_controller


    # standard workflow is that executor delegates work to front-end RPA
    def execute_decision(self, candidate: MailJobCandidate | ScheduledJobCandidate, decision: JobDecision, mail_backend: FolderMailBackend | None=None) -> dict | None:
        
        is_mail = isinstance(candidate, MailJobCandidate)
        is_scheduled = isinstance(candidate, ScheduledJobCandidate)

        if decision.ui_log_message:
            self.log_ui_and_system(decision.ui_log_message)

        if decision.system_log_message:
            self.log_system(decision.system_log_message)

        if is_mail:
            if mail_backend is None:
                raise ValueError("mail_backend required for mail actions")

            if decision.action == "MOVE_BACK_TO_INBOX": # eg. something's wrong with in-scoop emails from shared inbox
                mail_backend.move_back_to_inbox(candidate)
                return None

            if decision.action == "DELETE_ONLY":
                mail_backend.delete_from_processing(candidate) 
                return None
            
            if decision.action == "REPLY_AND_DELETE":
                if decision.reply_subject is None or decision.reply_body is None:
                    raise ValueError("action REPLY_AND_DELETE requires reply_subject and reply_body")
                job_id = self.generate_job_id()

                now = datetime.datetime.now()
                self.audit_repo.insert_job(
                    job_id=job_id,
                    email_address=candidate.sender_email,
                    email_subject=candidate.subject,
                    job_type=decision.job_type,
                    job_start_date=now.strftime("%Y-%m-%d"),
                    job_start_time=now.strftime("%H:%M:%S"),
                    job_finish_time=now.strftime("%H:%M:%S"),
                    job_status=decision.job_status,
                    error_code=decision.error_code,
                    error_message=decision.error_message,
                )
                #if the job could be DONE without handover, eg. "ping"
                if decision.job_status == "DONE":
                    self.refresh_jobs_done_today_display()

                mail_backend.reply_and_delete(
                    candidate,
                    subject=decision.reply_subject,
                    body=decision.reply_body,
                    job_id=job_id,
                )
    
                return None
        
        if is_scheduled:
            if decision.action == "SKIP":

                job_id = self.generate_job_id()
                now = datetime.datetime.now()

                self.audit_repo.insert_job(
                    job_id=job_id,
                    order_number=candidate.order_number,
                    job_type=decision.job_type,
                    job_start_date=now.strftime("%Y-%m-%d"),
                    job_start_time=now.strftime("%H:%M:%S"),
                    job_finish_time=now.strftime("%H:%M:%S"),
                    job_status=decision.job_status,
                    error_code=decision.error_code,
                    error_message=decision.error_message,
                )
                return None

        if decision.action == "QUEUE_RPA_JOB":
            job_id = self.generate_job_id()
            

            self.update_ui_status("working")

            if is_mail:
                if mail_backend is None: raise ValueError("mail_backend required for mail actions")
                
                # send lifesign notice, and only once a day per user (to avoid spam)
                if decision.send_lifesign_notice and not self.audit_repo.has_sender_job_today(candidate.sender_email):
                    mail_backend.send_reply(
                        mail=candidate,
                        subject = f"ONLINE re: {candidate.subject}",
                        body = (">HELLO HUMAN\n\n"
                        "This is an automated system reply.\n\n"
                        "It appears to be your first request today, so this reply confirms that the robot is online.\n"
                        "Your job has been received and is now processing.\n"
                        "You will receive another message when the job is completed."),
                        job_id=job_id,
                    )

            now = datetime.datetime.now()
            self.audit_repo.insert_job(
                job_id=job_id,
                email_address=candidate.sender_email if is_mail else None,
                email_subject=candidate.subject if is_mail else None,
                order_number=candidate.order_number if is_scheduled else None,
                job_type=decision.job_type,
                job_start_date=now.strftime("%Y-%m-%d"),
                job_start_time=now.strftime("%H:%M:%S"),
                job_status="QUEUED",
            )

            if decision.start_recording:
                if not self.in_dev_mode:
                    self.recording_service.start(job_id)
                self.ui_dot_tk_set_show_recording_overlay()

            if decision.handover_payload is None: raise RuntimeError("handover_payload is None for QUEUE_RPA_JOB")

            handover_data = {
                "ipc_state": "job_queued",
                "job_id": job_id,
                **decision.handover_payload,
            }

            return handover_data

        if decision.action == "CRASH":
            job_id = self.generate_job_id()
            now = datetime.datetime.now()

            try:
                self.audit_repo.insert_job(
                    job_id=job_id,
                    email_address=candidate.sender_email if is_mail else None,
                    email_subject=candidate.subject if is_mail else None,
                    order_number=candidate.order_number if is_scheduled else None,
                    job_type=decision.job_type,
                    job_start_date=now.strftime("%Y-%m-%d"),
                    job_start_time=now.strftime("%H:%M:%S"),
                    job_finish_time=now.strftime("%H:%M:%S"),
                    job_status="FAIL",
                    error_code="SYSTEM_CRASH",
                    error_message=decision.crash_reason,
                )
            except Exception:
                pass
                
            if is_mail:
                try:                    
                    if mail_backend is None: raise ValueError("mail_backend required for mail actions")
                    mail_backend.reply_and_delete(
                        candidate,
                        subject=f"FAIL re: {candidate.subject}",
                        body="System crash, the robot is now out-of-service and your email was deleted.",
                        job_id=job_id,
                    )
                except Exception:
                    pass
            


            self.log_ui_and_system("--> rejected (system crash)")
            self.safestop_controller.enter_safestop(reason=decision.crash_reason, job_id=job_id)
            return None

        raise ValueError(f"decision.action={decision.action} is not valid for specified candidate type")


# for closing the job
class PostHandoverFinalizer:
    ''' the verification step, if any, is always a cold start '''
    def __init__(self, log_system, log_ui_and_system, audit_repo, job_handlers, recording_service, ui_dot_tk_set_hide_recording_overlay, refresh_jobs_done_today_display, in_dev_mode) -> None:
        self.in_dev_mode = in_dev_mode

        self.log_system = log_system
        self.log_ui_and_system = log_ui_and_system
        self.audit_repo = audit_repo
        self.job_handlers = job_handlers
        self.recording_service = recording_service
        self.ui_dot_tk_set_hide_recording_overlay = ui_dot_tk_set_hide_recording_overlay
        self.refresh_jobs_done_today_display = refresh_jobs_done_today_display
        self.mail_backend_personal = FolderMailBackend(self.log_system, pipeline_root="personal_inbox",  )
        self.mail_backend_shared = FolderMailBackend(self.log_system, pipeline_root="shared_inbox",  )


    def poll_once(self, handover_data) -> None:
        time.sleep(1) #simulate verification time

        job_id = handover_data.get("job_id")
        job_type = handover_data.get("job_type")

        # note in audit that the job is 'taken back' from front-end RPA
        self.log_system(f"fetched: {handover_data}", job_id)
        self.audit_repo.update_job(
            job_id=job_id,
            job_status="VERIFYING"
            )

        # cold start needs to rebuild candidate objekt
        candidate = self.rebuild_candidate(handover_data)

        # use job-specific verfification 
        handler = self.job_handlers.get(job_type)
        if handler is None:
            result= f"No handler for job_type={job_type}"

        else:
            try:
                result = handler.verify_result(candidate, job_id)
            except Exception as err:
                result = f"verification crash: {err}"

        self.finalize_job_result(handover_data, result, candidate)

    

    def finalize_job_result(self, handover_data, result, candidate: MailJobCandidate | ScheduledJobCandidate):
        
        if result == "ok":
            job_status = "DONE"
            error_code = None
            error_message = None
        else:
            job_status = "FAIL"
            error_message = result
            error_code="VERIFICATION_FAIL"

        
        job_id = handover_data.get("job_id")

        # update audit w/ result (DONE/FAIL)
        self.audit_repo.update_job(
            job_id=job_id, 
            job_status=job_status, 
            error_code=error_code, 
            error_message=error_message, 
            job_finish_time=datetime.datetime.now().strftime("%H:%M:%S")) 


        job_type = handover_data.get("job_type")
        self.log_ui_and_system(f"--> {job_status.lower()} ({job_type})", job_id)

        self.recording_service.stop(job_id) 
        self.ui_dot_tk_set_hide_recording_overlay() # the " *RECORDING "-box

        if not self.in_dev_mode: self.recording_service.upload_recording(job_id=job_id)

        self.refresh_jobs_done_today_display()

        # if the job was an email, delete it
        if isinstance(candidate, MailJobCandidate):
            if candidate.job_source_type == "personal_inbox":
                self.send_final_job_reply(candidate, job_id=job_id, job_status=job_status)

            mail_backend = self.get_mail_backend_for_candidate(candidate)
            mail_backend.delete_from_processing(candidate, job_id=job_id)



        if not result == "ok":
            raise RuntimeError(f"verification failed: {result}")
        

    def rebuild_candidate(self, handover_data: dict) -> MailJobCandidate | ScheduledJobCandidate:
        # rebuild object after cold start
        source_type = handover_data.get("job_source_type")

        if source_type in ("personal_inbox", "shared_inbox"):

            message_ref_raw = handover_data.get("message_ref")
            assert message_ref_raw is not None

            return MailJobCandidate(
                message_id=str(handover_data.get("message_id")),
                sender_email=str(handover_data.get("sender_email")),
                sender_name=str(handover_data.get("sender_name")),
                subject=str(handover_data.get("subject")),
                body=str(handover_data.get("body")),
                headers={}, #fix this
                message_ref=Path(message_ref_raw),
                job_source_type=source_type,
                )
        

        if source_type == "erp_query":

            order_number = handover_data.get("order_number")
            if order_number is None:
                raise ValueError("missing order_number")

            return ScheduledJobCandidate(
                order_number=int(order_number),
                order_qty=int(handover_data.get("order_qty", 0)),
                material_available=int(handover_data.get("material_available", 0)),
                job_source_type="erp_query",
            )
        
        raise ValueError(f"unknown job_source_type={source_type!r}")


    def get_mail_backend_for_candidate(self, mail: MailJobCandidate) -> FolderMailBackend:
        if mail.job_source_type == "personal_inbox":
            return self.mail_backend_personal
        if mail.job_source_type == "shared_inbox":
            return self.mail_backend_shared
        raise ValueError(f"unknown job_source_type={mail.job_source_type}")
    
    
    def send_final_job_reply(self, candidate, job_id, job_status) -> None:
        # stub. move to verify?

        
        #stub
        success_body =f"Job completed successfully. Screen-recording can be found >link<. Jobid {job_id}" 
        fail_body =" fail because of ... "

        subject = f"{job_status} re: {candidate.subject}"
        body=f"{success_body} \n_ _ _ _ _ _ _ _ _ _ _ \n{candidate.body}"

        self.mail_backend_personal.send_reply(candidate, subject, body, job_id=job_id)


    def generate_final_email_reply(self):
        pass


# for file-IPC
class HandoverRepository:
    ''' handover.json is the I/O between this script and the front-end RPA  '''

    JOB_TYPES = ("ping", "job1", "job2", "job3", "job4")
    IPC_STATES = ("idle", "job_queued", "job_running", "job_verifying", "safestop")
    
    def __init__(self, log_system) -> None:
        self.log_system = log_system

   
    def read(self) -> dict:
        ''' read handover.json '''

        last_err=None

        for attempt in range(7):
            try:
                with open("handover.json", "r", encoding="utf-8") as f:
                    handover_data = json.load(f)

                self.validate_handover_data(handover_data)
                return handover_data

            except Exception as err:
                last_err = err
                print(f"WARN: retry {attempt+1}/7 : {err}")
                #time.sleep((attempt+1) ** 2) fail fast in dev
        
        raise RuntimeError(f"handover.json unreadable: {last_err}")
    
      
    def write(self, handover_data: dict, file="handover.json") -> None:
        ''' atomic write of handover.json '''

        self.validate_handover_data(handover_data)

        for attempt in range(7):
            temp_path = None
            try:
                dir_path = os.path.dirname(os.path.abspath(file))
                fd, temp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")    # create temp file

                #atomic write
                with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                    json.dump(handover_data, tmp, indent=2) # indent for human eyes
                    tmp.flush()
                    os.fsync(tmp.fileno())

                os.replace(temp_path, file) # replace original file
                self.log_system(f"written: {handover_data}", job_id=handover_data.get("job_id"))
                return

            except Exception as err:
                last_err = err
                print(f"{attempt+1}st warning from write()")
                self.log_system(f"WARN: {attempt+1}/7 error", job_id=handover_data.get("job_id"))
                #time.sleep(attempt + 1) # 1 2... 7 sec       #fail fast in dev

            finally: #remove temp-file if writing fails.
                if temp_path and os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except Exception: pass

        self.log_system(f"CRITICAL: cannot write handover.json {last_err}", job_id=handover_data.get("job_id"))
        raise RuntimeError("CRITICAL: cannot write handover.json")
  

    def validate_handover_data(self, handover_data) -> None:
        
        ''' check some basic combinations '''

        ipc_state = handover_data.get("ipc_state")
        job_id = handover_data.get("job_id")
        job_type = handover_data.get("job_type")
        job_source_type = handover_data.get("job_source_type")


        if ipc_state not in self.IPC_STATES:
            raise ValueError(f"unknown state: {ipc_state}")
        
        if ipc_state in ("job_queued", "job_running", "job_verifying"):
            if not job_id:
                raise ValueError(f"job_id missing for {ipc_state}")
            if not job_type:
                raise ValueError(f"job_type missing for {ipc_state}")
            if job_type not in self.JOB_TYPES:
                raise ValueError(f"unkown job_type: {job_type} for {ipc_state}")
        
       
       
        # mail specific checks, all fields are required

        if job_source_type in ("personal_inbox", "shared_inbox"):
            
            message_id = handover_data.get("message_id")
            sender_email = handover_data.get("sender_email")
            sender_name = handover_data.get("sender_name")
            subject = handover_data.get("subject")
            body = handover_data.get("body")
            headers = handover_data.get("headers") # attached files?
            message_ref = handover_data.get("message_ref") # datatype Path in dev

            required_fields = {
            "message_id": message_id,
            "sender_email": sender_email,
            "sender_name": sender_name,
            "subject": subject,
            "body": body,
            #"headers": headers,
            "message_ref": message_ref,
            }

            missing = [k for k, v in required_fields.items() if not v]

            if missing:
                raise ValueError(f"missing fields in handover.json: {missing}")
            
            
            # datatype Path in dev
            try:
                message_ref = Path(message_ref)
            except TypeError:
                raise TypeError("message_ref must be path-like")
                           
# for screen-recording
class RecordingService:
    ''' screen-recording to cature all front-end RPA screen-activity '''

    def __init__(self, log_system,) -> None:
        self.RECORDINGS_IN_PROGRESS_FOLDER = "recordings_in_progress"
        self.RECORDINGS_DESTINATION_FOLDER = "recordings_destination"

        self.log_system = log_system
        self.recording_process = None

    #start the recording
    def start(self, job_id) -> None:
        #written by AI
        
        os.makedirs(self.RECORDINGS_IN_PROGRESS_FOLDER, exist_ok=True)
        filename = f"{self.RECORDINGS_IN_PROGRESS_FOLDER}/{job_id}.mkv"

        drawtext = (
            f"drawtext=text='job_id  {job_id}':"
            "x=200:y=20:"
            "fontsize=32:"
            "fontcolor=lightyellow:"
            "box=1:"
            "boxcolor=black@0.5"
        )

        if platform.system() == "Windows":
            capture = ["-f", "gdigrab", "-i", "desktop"]
            ffmpeg = "./ffmpeg.exe"
            if not os.path.exists(ffmpeg): raise RuntimeError ("screen-recording file ffmpeg.exe is missing, download from eg. https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.7z and place it next to this script.") 
            recording_process = subprocess.Popen(
                [ffmpeg, "-y", *capture, "-framerate", "15", "-vf", drawtext,
                "-vcodec", "libx264", "-preset", "ultrafast", filename],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            )
        else:
            capture = ["-video_size", "1920x1080", "-f", "x11grab", "-i", ":0.0"]
            ffmpeg = "ffmpeg"
            recording_process = subprocess.Popen(
                [ffmpeg, "-y", *capture, "-framerate", "15", "-vf", drawtext,
                "-vcodec", "libx264", "-preset", "ultrafast", filename],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
        time.sleep(0.2) #adding dummy time to start the recording
        
        self.recording_process = recording_process  
        self.log_system("recording started", job_id)
  
    #stop recording
    def stop(self, job_id=None) -> None:
        #written by AI
        try:
            self.log_system("stop recording", job_id)
        except Exception: pass

        recording_process = self.recording_process
        self.recording_process = None

        try:
            if recording_process is not None:
                if platform.system() == "Windows":
                    try:
                        recording_process.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
                    except Exception:
                        recording_process.terminate()

                    try:
                        recording_process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        subprocess.run(
                            ["taskkill", "/IM", "ffmpeg.exe", "/T", "/F"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            check=False,
                        )

                else:
                    try:
                        os.killpg(recording_process.pid, signal.SIGINT)
                    except Exception:
                        recording_process.terminate()

                    try:
                        recording_process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        subprocess.run(
                            ["killall", "-q", "-KILL", "ffmpeg"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            check=False,
                        )
            else:
                # fallback if proc-object tappats bort
                if platform.system() == "Windows":
                    subprocess.run(
                        ["taskkill", "/IM", "ffmpeg.exe", "/T", "/F"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        check=False,
                    )
                else:
                    subprocess.run(
                        ["killall", "-q", "-KILL", "ffmpeg"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        check=False,
                    )

        except Exception as err:
            print("WARN from stop():", err)

    #upload to a shared drive
    def upload_recording(self, job_id, max_attempts=3) -> bool:
    
        local_file = f"{self.RECORDINGS_IN_PROGRESS_FOLDER}/{job_id}.mkv"
        local_file = Path(local_file)

        remote_path = Path(self.RECORDINGS_DESTINATION_FOLDER) / f"{job_id}.mkv"
        remote_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(max_attempts):
            try:
                
                shutil.copy2(local_file, remote_path)
                #print(f"✓ Upload successful: {remote_path}")
                self.log_system(f"upload success: {remote_path}", job_id)
                try: os.remove(local_file)
                except Exception: pass

                return True

            except Exception as e:
                wait_time = (attempt + 1) ** 2
                print(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
                time.sleep(wait_time)
        
        self.log_system(f"upload failed: {remote_path}", job_id)
        return False

    # cleanup aborted screen-recordings
    def cleanup_aborted_recordings(self):

        directory = Path(self.RECORDINGS_IN_PROGRESS_FOLDER)
        if not directory.exists():
            return
        
        for file in directory.iterdir():

            if file.is_file() and file.suffix == ".mkv":
                job_id = file.stem
                
                try:
                    self.upload_recording(job_id)
                    self.log_system(f"cleanup upload of {job_id}") #add recovery procedure for jo_id?
                except Exception as err:
                    self.log_system(f"cleanup failed for {job_id}: {err}")

# for access
class FriendsRepository:
    ''' friends.xlsx contains user access to use 'personal_inbox' '''
    def __init__(self, log_system) -> None:
        self.log_system = log_system
        self.access_by_email = {}
        self.access_file_mtime = None


    def ensure_friends_file_exists(self, path="friends.xlsx") -> None:
        ''' Makes a template if no friends.xlsx '''
        if os.path.exists(path):
            return

        wb = Workbook()
        ws = wb.active
        assert ws is not None

        # headers
        ws["A1"] = "email"
        ws["B1"] = "ping"
        ws["C1"] = "job1"
        ws["D1"] = "job2"

        # rows
        ws["A2"] = "alice@example.com"
        ws["B2"] = "x"

        ws["A3"] = "bob@test.com"
        ws["B3"] = "x"
        ws["C3"] = "x"
        ws["D3"] = "x"

        wb.save(path)
        wb.close()
    

    def load_access_map(self, filepath="friends.xlsx") -> dict:
        #code written by AI
        '''
        Reads friends.xlsx and returns eg.:

        {
            "alice@example.com": {"ping"},
            "ex2@whatever.com": {"ping", "job1"}
        }

        Presumptions:
        A1 = email
        row 1 contains job_type
        'x' gives access
        '''
        wb = load_workbook(filepath, data_only=True)
        try:
            ws = wb.active
            assert ws is not None

            rows = list(ws.iter_rows(values_only=True))
            if len(rows) < 2:
                raise ValueError("friends.xlsx contains no users")

            header = rows[0]
            access_map: dict[str, set[str]] = {}

            for row in rows[1:]:
                email_cell = row[0]
                if email_cell is None:
                    continue

                email = str(email_cell).strip().lower()
                if not email:
                    continue

                permissions = set()

                for col in range(1, len(header)):
                    jobname = header[col]
                    if jobname is None:
                        continue

                    jobname = str(jobname).strip().lower()
                    cell = row[col] if col < len(row) else None

                    if cell is None:
                        continue

                    if str(cell).strip().lower() == "x":
                        permissions.add(jobname)

                access_map[email] = permissions

            return access_map
        finally:
            wb.close()


    def reload_if_modified(self, force_reload=False, filepath="friends.xlsx") -> bool:
        #code written by AI
        '''      reload friends.xlsx if changed.       '''

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"{filepath} not found")

        current_mtime = os.path.getmtime(filepath)

        if (not force_reload) and (self.access_file_mtime == current_mtime):
            return False   # ingen ändring

        new_access = self.load_access_map(filepath)

        self.access_by_email = new_access
        self.access_file_mtime = current_mtime

        return True


    def is_allowed_sender(self, email_address: str) -> bool:
        email_address = email_address.strip().lower()
        result = email_address in self.access_by_email
        self.log_system(f"returning: {result}")
        return result


    def has_job_access(self, email_address: str, job_type: str) -> bool:
        email_address = email_address.strip().lower()
        job_type = job_type.strip().lower()
        result = job_type in self.access_by_email.get(email_address, set())
        self.log_system(f"returning: {result}")
        return result

# for network-check (note to self: check behaviour)
class NetworkService:
    ''' checks if the computer is connected to company LAN '''
    NETWORK_HEALTHCHECK_PATH = r"/" #enter path to network drive here, e.g. "G:\"

    def __init__(self, log_system) -> None:
        self.log_system = log_system
        self.network_state = False #assume offline at start
        self.next_network_check_time = 0


    def has_network_access(self) -> bool:
        #this runs at highest once every hour (if online), or before new jobs

        now = time.time()

        if now < self.next_network_check_time:
            return self.network_state

        try:
            os.listdir(self.NETWORK_HEALTHCHECK_PATH)
            online = True
            
        except Exception:
            online = False
            

        # update log if any network change (and UI? )
        if online != self.network_state:
            self.network_state = online

            if online:
                self.log_system("network restored")
            else:
                self.log_system(f"WARN: network lost")

        # check every minute if offline, else every hour (??)
        if online:
            self.next_network_check_time = now + 3600   # 1 h
        else:
            self.next_network_check_time = now + 60     # 1 min
        
        return online

# for SQLite
class AuditRepository:
    ''' handles job_audit.db, an audit-style robot activity log '''
    def __init__(self, log_system) -> None:
        self.log_system = log_system
        

    def ensure_db_exists(self) -> None:
        
        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()
           
            cur.execute('''
                CREATE TABLE IF NOT EXISTS audit_log
                         (
                        job_id INTEGER PRIMARY KEY, 
                        job_type TEXT, 
                        job_status TEXT, 
                        email_address TEXT, 
                        email_subject TEXT, 
                        order_number INTEGER,
                        job_start_date TEXT, 
                        job_start_time TEXT, 
                        job_finish_time TEXT, 
                        final_reply_sent INTEGER NOT NULL DEFAULT 0, 
                        error_code TEXT, 
                        error_message TEXT 
                        )
                        ''')
        conn.close()


    def insert_job(self, job_id, email_address=None, email_subject=None, order_number=None, job_type=None, job_start_date=None, job_start_time=None, job_finish_time=None, job_status=None, final_reply_sent=None, error_code=None,error_message=None,) -> None:
        # used for new row
        if job_status not in ("REJECTED", "QUEUED", "RUNNING", "VERIFYING", "DONE", "FAIL", None):
            raise ValueError(f"update_job(): unknown job_status={job_status} for INSERT INTO")

        all_fields = {
            "job_id": job_id,
            "email_address": email_address,
            "email_subject": email_subject,
            "order_number": order_number,
            "job_type": job_type,
            "job_start_date": job_start_date,
            "job_start_time": job_start_time,
            "job_finish_time": job_finish_time,
            "job_status": job_status,
            "final_reply_sent": final_reply_sent,
            "error_code": error_code,
            "error_message": error_message,
        }

        fields = {k: v for k, v in all_fields.items() if v is not None}
        self.log_system(f"received fields: {fields}", job_id=job_id)
        
        columns = ", ".join(fields.keys())
        placeholders = ", ".join("?" for _ in fields)


        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()

            cur.execute(
                f"INSERT INTO audit_log ({columns}) VALUES ({placeholders})",
                tuple(fields.values())
            )
        conn.close()


    def update_job(self, job_id, email_address=None, email_subject=None, order_number=None, job_type=None, job_start_date=None, job_start_time=None, job_finish_time=None, job_status=None, final_reply_sent=None, error_code=None,error_message=None,) -> None:
        # example use: self.audit_repo.update_job(job_id=20260311124501, job_type="job1")

        if job_status not in ("REJECTED", "QUEUED", "RUNNING", "VERIFYING", "DONE", "FAIL", None):
            raise ValueError(f"update_job(): unknown job_status={job_status}")

        all_fields = {
            "job_id": job_id,
            "email_address": email_address,
            "email_subject": email_subject,
            "order_number": order_number,
            "job_type": job_type,
            "job_start_date": job_start_date,
            "job_start_time": job_start_time,
            "job_finish_time": job_finish_time,
            "job_status": job_status,
            "final_reply_sent": final_reply_sent,
            "error_code": error_code,
            "error_message": error_message,
        }

        # disregard None-fields
        fields = {k: v for k, v in all_fields.items() if v is not None}
        self.log_system(f"received fields: {fields}", job_id=job_id)

        fields.pop("job_id", None)

        if not fields:
            return

        set_clause = ", ".join(f"{k}=?" for k in fields)


        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()

            cur.execute(
                f"UPDATE audit_log SET {set_clause} WHERE job_id=?",
                (*fields.values(), job_id)
            )

            if cur.rowcount == 0:
                raise ValueError(f"update_job(): no row in DB with job_id={job_id}")
        conn.close()


    
    def count_completed_jobs_today(self) -> int:
        # used for UI dash

        today = datetime.date.today().isoformat()

        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT COUNT(*)
                FROM audit_log
                WHERE job_start_date = ?
                AND job_status = 'DONE'
            ''', (today,))
            
            result = cur.fetchone()[0]
        conn.close()

        return result

    # used to send max one notification-response a day
    def has_sender_job_today(self, email_address) -> bool:    

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()

            cur.execute(
                '''
                SELECT COUNT(*)
                FROM audit_log
                WHERE job_start_date = ? AND email_address = ?
                ''',
                (today, email_address,)
            )

            jobs_today = cur.fetchone()[0]
        conn.close()

        self.log_system(f"returning: {jobs_today > 0}")

        return jobs_today > 0


    def has_been_processed_today(self, order_number) -> bool:
        # used to avoid bad loops in schedule-jobs

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()

            cur.execute(
                '''
                SELECT COUNT(*)
                FROM audit_log
                WHERE job_start_date = ? AND order_number = ?
                ''',
                (today, order_number,)
            )

            jobs_today = cur.fetchone()[0]
        conn.close()

        #self.log_system(f"returning {order_number} is  {jobs_today > 0}")
        return jobs_today > 0


    # used to avoid conflicting job_id
    def get_latest_job_id(self) -> int:
        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT job_id
                FROM audit_log
                ORDER BY job_id DESC
                LIMIT 1
            ''')
            row = cur.fetchone()
        conn.close()

        return row[0] if row is not None else 0


    def get_failed_jobs(self, days=7):
        # impelement in UI dash ?
        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT job_id, email_address, job_type, error_code, error_message
                FROM audit_log
                WHERE job_status = 'FAIL'
                AND job_start_date >= date('now', '-' || ? || ' days')
                ORDER BY job_id DESC
            ''', (days,))
        res = cur.fetchall()
        conn.close()
        
        return res


    def has_unreplied_finished_jobs(self) -> bool:
        # implement...
        with sqlite3.connect("job_audit.db") as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT job_id
                FROM audit_log
                WHERE final_reply_sent = 0
                AND job_status IN ('DONE', 'FAIL', 'REJECTED')
                LIMIT 1
                ''')
        result = cur.fetchone() is not None
        conn.close()
        return result

# for job: ping
class ExamplePingJobHandler:
    def __init__(self,log_system) -> None:
        self.log_system = log_system

    def precheck_and_build_payload(self, mail: MailJobCandidate) -> tuple[bool, dict | str]:
        return True, {}
    
    def play_sound(self) -> None:
        
        system = platform.system()

        if system == "Windows":
            import winsound
            # frequency (Hz), duration (ms)
            winsound.Beep(1000, 300) #type: ignore

        elif system == "Linux":
            print("\a", end="", flush=True)

# for job1 (stub)
class ExampleJob1Handler:
    ''' everything for "job1" '''
    def __init__(self,log_system) -> None:
        self.log_system = log_system


    # sanity-check on the given data, eg. are all fields supplied and in correct format?
    def precheck_and_build_payload(self, mail: MailJobCandidate) -> tuple[bool, dict | str]:
        body = mail.body

        sku_match = re.search(r"SKU:\s*(.+)", body)
        sku = sku_match.group(1) if sku_match else None

        old_material_match = re.search(r"Old material:\s*(.+)", body)
        old_material = old_material_match.group(1) if old_material_match else None

        new_material_match = re.search(r"New material:\s*(.+)", body)
        new_material = new_material_match.group(1) if new_material_match else None

        error = ""
        if sku is None:
            error += "missing SKU. "
        if old_material is None:
            error += "missing Old material. "
        if new_material is None:
            error += "missing New material. "

        if error:
            return False, error.strip()

        payload = {
            "sku": sku,
            "old_material": old_material,
            "new_material": new_material,
        }

        return True, payload
    

    def verify_result(self, candidate: MailJobCandidate, job_id):
        return "ok"

# job2... (stub)
class ExampleJob2Handler:
    def __init__(self,log_system) -> None:
        self.log_system = log_system

    
    def precheck_and_build_payload(self, mail: MailJobCandidate) -> tuple[bool, dict | str]:
        return False, "Missing required fields for job2."

    def verify_result(self, candidate: MailJobCandidate, job_id):
        return "ok"
    
# job3...(some content)
class ExampleJob3Handler:
    ''' everything for job3 '''
    def __init__(self, log_system) -> None:
        self.log_system = log_system
        self.excel_erp_backend = ExcelErpBackend()

   
    def precheck_and_build_payload(self, row: ScheduledJobCandidate) -> tuple[bool, dict | str]:
        result = {"stub": "stub."}
        return True, result
    

    def verify_result(self, candidate: ScheduledJobCandidate, job_id) -> str:
        
        order_number = candidate.order_number
        if not order_number:
            return "missing order_number"

        order_qty = self.excel_erp_backend.get_order_qty(order_number)
        self.log_system(f"order_qty in ERP is now: {order_qty}", job_id)
        if order_qty is None:
            return f"order {order_number} not found"

        if order_qty != candidate.material_available:
            return "ERP still shows mismatch after RPA update"

        return "ok"

# for asking an LLM (not implemented)
class AIHelper:
    def prompt(self, input: str, question: str) -> str:

        header = "You are an agent in an RPA application."
        f"This is the file, in str format, that the question regards: {input}."
        f"The question is: {question}."

        ai_reply = "im not implemented"
        return ai_reply

# for crash-mode
class SafeStopController:
    def __init__(self, log_system, log_ui_and_system, recording_service, ui, mail_backend, audit_repo, generate_job_id, friends_repo) -> None:
        self.log_system = log_system
        self.log_ui_and_system = log_ui_and_system
        self.recording_service = recording_service
        self.ui = ui
        self.mail_backend = mail_backend
        self.audit_repo = audit_repo
        self.generate_job_id = generate_job_id
        self.friends_repo = friends_repo
        self._safestop_entered = False


    def enter_safestop(self, reason, job_id=None) -> None:
        # all critical errors end up here. very defensive
        
        if self._safestop_entered: return #re-entrancy protection
        self._safestop_entered = True 

        print("ROBOTRUNTIME CRASHED:\n", reason) 

        try: self.log_system(f"ROBOTRUNTIME CRASHED: {reason}", job_id)
        except Exception: pass

        try: self.send_admin_alert(reason)
        except Exception: pass

        try: self.log_ui_and_system("CRASH! All automations halted. Admin is notified.", blank_line_before=True)
        except Exception: pass

        # implement check to make sure reply is sent, if emailjob ?
        #try:
        #    if job_id is not None:
        #        self.incoming_mail_handler.send_final_job_reply(job_id, status="FAIL")
        #except Exception: pass

        try: self.recording_service.stop()
        except Exception: pass

        try: self.ui.tk_set_hide_recording_overlay()
        except Exception: pass

        try: self.ui.tk_set_status("safestop")
        except Exception: # rather kill than allow UI dash freeze
            try: self.ui.tk_set_shutdown()
            except: os._exit(1)
            
            time.sleep(3)
            os._exit(0)
        
        self.run_degraded_mode()


    def run_degraded_mode(self, rebootflag="reboot.flag") -> Never:
        # reject all mail to 'personal_inbox' and wait for reboot-command
        try: self.log_system("running")
        except Exception: pass
        
        while True:
            try:
                time.sleep(1)

                if os.path.isfile(rebootflag):
                    try: os.remove(rebootflag)
                    except Exception: pass
                    try: self.log_system(f"reboot-command received from {rebootflag}")
                    except Exception: pass
                    self.restart_application()

                inbox_path = self.mail_backend.fetch_next_from_inbox()
                if not inbox_path:
                    continue

                processing_path = self.mail_backend.claim_to_processing(inbox_path)
                mail = self.mail_backend.parse_processing_mail(processing_path)
                self.log_ui_and_system(f"email from {mail.sender_email}", blank_line_before=True)

                if not self.friends_repo.is_allowed_sender(mail.sender_email):
                    self.log_ui_and_system("--> rejected (not in friends.xlsx)")
                    self.mail_backend.delete_from_processing(mail)
                    continue
   
                if "reboot1234" in mail.subject.strip().lower():
                    try: self.log_system(f"reboot command received from {mail.sender_email}")
                    except Exception: pass
                    try: self.mail_backend.reply_and_delete(mail, subject=f"got it! re: {mail.subject}", body="Reboot command received")
                    except Exception: pass
                    self.restart_application()
                
                elif "stop1234" in mail.subject.strip().lower():
                    try: self.log_system(f"stop command received from {mail.sender_email}")
                    except Exception: pass
                    try: self.mail_backend.reply_and_delete(mail, subject=f"got it! re: {mail.subject}", body="Stop-command received, shutting down.. it's getting dark...")
                    except Exception: pass
                    try: self.ui.tk_set_shutdown()
                    except Exception: os._exit(1)
                    os._exit(0)

                
                try: self.mail_backend.reply_and_delete(mail, subject=f"FAIL re: {mail.subject}", body="Robot is out-of-service. Your email was deleted.")
                except Exception: pass
                try:
                    job_id = self.generate_job_id()
                    now = datetime.datetime.now()
                    self.audit_repo.insert_job(
                        job_id=job_id,
                        email_address=mail.sender_email,
                        email_subject=mail.subject,
                        job_start_date=now.strftime("%Y-%m-%d"),
                        job_start_time=now.strftime("%H:%M:%S"),
                        job_status="REJECTED",
                        error_code="SAFESTOP",
                    )
                except Exception: pass
                try: self.log_ui_and_system("--> rejected (safestop)")
                except Exception: pass
            
            except Exception as err:
                try: self.log_system(f"err: {err}")
                except Exception: pass


    def restart_application(self) -> Never:
        # this really works on UI dash freeze?
        try:
            self.ui.tk_set_shutdown()
        except Exception:
            pass

        try:
            subprocess.Popen(
                [sys.executable, *sys.argv],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
        except Exception:
            os._exit(1)

        time.sleep(3)
        os._exit(0)


    def send_admin_alert(self, reason):
         mail=MailJobCandidate(
            message_id="dummy",
            sender_email="adminATcompany.com",
            subject="safestop entered",
            body=f"Reson for safestop: {reason}",
            sender_name="backend-RPA",
            headers={},
            message_ref=Path("safestop, no real path"),
            job_source_type="personal_inbox"
            )
         
        # maybe build a dedicated sender instead, since this is not a reply
         self.mail_backend.send_reply(mail, subject=mail.subject, body=mail.body)
                
# for core orchestration logic - "the brain"
class RobotRuntime:

    def __init__(self, ui):

        self.in_dev_mode = True

        self.ui = ui
        self.handover_repo = HandoverRepository(self.log_system)  #nu äger Runtime en handover_repo (som får med append-metod)
        self.friends_repo = FriendsRepository(self.log_system)
        self.audit_repo = AuditRepository(self.log_system)
        self.network_service = NetworkService(self.log_system)
        self.recording_service = RecordingService(self.log_system)
        self.safestop_controller = SafeStopController(self.log_system, self.log_ui_and_system, self.recording_service, ui, FolderMailBackend(self.log_system, pipeline_root="personal_inbox"), self.audit_repo, self.generate_job_id, self.friends_repo) 
        self.job_handlers = {
            "ping": ExamplePingJobHandler(self.log_system),
            "job1": ExampleJob1Handler(self.log_system), 
            "job2": ExampleJob2Handler(self.log_system), 
            "job3": ExampleJob3Handler(self.log_system),}
    
        self.pre_handover_executor = PreHandoverExecutor(log_system=self.log_system, log_ui_and_system=self.log_ui_and_system, update_ui_status=self.update_ui_status, refresh_jobs_done_today_display=self.refresh_jobs_done_today_display, ui_dot_tk_set_show_recording_overlay=self.ui.tk_set_show_recording_overlay, generate_job_id=self.generate_job_id, recording_service=self.recording_service, audit_repo=self.audit_repo, safestop_controller=self.safestop_controller, in_dev_mode=self.in_dev_mode)
        self.scheduled_flow = ScheduledFlow(log_system=self.log_system, log_ui_and_system=self.log_ui_and_system, audit_repo=self.audit_repo, job_handlers=self.job_handlers, in_dev_mode=self.in_dev_mode, pre_handover_executor=self.pre_handover_executor)
        self.mail_flow = MailFlow(self.log_system, self.log_ui_and_system, self.friends_repo, self.is_within_operating_hours, self.network_service, self.job_handlers, self.pre_handover_executor)
        self.post_handover_filalizer = PostHandoverFinalizer(self.log_system, self.log_ui_and_system, self.audit_repo, self.job_handlers, self.recording_service, self.ui.tk_set_hide_recording_overlay, self.refresh_jobs_done_today_display, self.in_dev_mode)

        
    def initialize_runtime(self):
        
        VERSION = 0.4
        self.log_system(f"RuntimeThread started, version={VERSION}")

        self.handover_repo.write({"ipc_state":"idle"}) # no-resume policy, always cold start

        # cleanup
        for fn in ["stop.flag", "reboot.flag"]:
            try: os.remove(fn)
            except Exception: pass

        self.network_service.has_network_access()

        atexit.register(self.recording_service.stop) #extra protection during normal python exit
        self.recording_service.stop() #stop any remaing recordings
        self.recording_service.cleanup_aborted_recordings()

        self.friends_repo.ensure_friends_file_exists()

        self.friends_repo.reload_if_modified(force_reload=True)

        self.audit_repo.ensure_db_exists()

        self.refresh_jobs_done_today_display()

        # not completed yet
        result = self.audit_repo.has_unreplied_finished_jobs()
        print("has_unreplied_finished_jobs", result)


    def run(self) -> None:
        
        try: self.initialize_runtime()
        except Exception as err: self.safestop_controller.enter_safestop(reason=err)

        self.prev_ui_status = None
        prev_ipc_state = None
        watchdog_started_at = None
        watchdog_timeout = 600 #600 for 10 min
        if self.in_dev_mode: watchdog_timeout = 10

        poll_interval = 1   # inverval for each cycle    

        while True:
            try:
                
                handover_data = self.handover_repo.read()
                ipc_state = handover_data.get("ipc_state")
                
                #dispatch
                if ipc_state == "idle":
                    self.check_for_jobs()           # sets ipc_state='job_queued' if front-end RPA needed
                    time.sleep(poll_interval)

                elif ipc_state == "job_queued":     # used by front-end RPA
                    time.sleep(poll_interval)

                elif ipc_state == "job_running":    # used by front-end RPA
                    time.sleep(poll_interval)

                elif ipc_state == "job_verifying":  # signal to orchestrator (from front-end RPA) to re-take the command
                    self.finalize_current_job(handover_data)

                elif ipc_state == "safestop":       # signal to orchestrator (from front-end RPA) that an error occured 
                    self.safestop_controller.enter_safestop(reason="crash_in_frontend_rpa", job_id=handover_data.get("job_id"))
                    

                # log all ipc_state transitions
                if ipc_state != prev_ipc_state:
                    self.log_system(f"state transition detected by CPU-poll: {prev_ipc_state} -> {ipc_state}")
                    self.update_ui_status(ipc_state)
                    print("state is", ipc_state)

                    # note handover time or last front-end RPA activity
                    if ipc_state in ("job_queued", "job_running"):
                        watchdog_started_at = time.time()
                    else:
                        watchdog_started_at = None
                   
                    # update DB when/if front-end RPA starts the job
                    if ipc_state == "job_running":
                        self.audit_repo.update_job(job_id=handover_data.get("job_id"), job_status="RUNNING")
                    

                # initiate crash if front-end RPA takes too long (to start or finish)
                if watchdog_started_at and ipc_state in ("job_queued", "job_running") and time.time() - watchdog_started_at > watchdog_timeout:
                    self.audit_repo.update_job(
                        job_id=handover_data.get("job_id"),
                        job_status="FAIL",
                        error_code="TIMEOUT",
                        error_message="No progress for 10 minutes",
                    )
                    watchdog_started_at = None
                    self.safestop_controller.enter_safestop(reason="RPA timeout - no progress for 10 min", job_id=handover_data.get("job_id"))
                    # self.incoming_mail_handler.reply_and_delete(message_id=handover_data.get("message_id"), job_id=handover_data.get("job_id"), message="The robot could not complete your job because the automation timed out. The job has failed and an administrator has been notified. Watch the screen-recording")    
                
                prev_ipc_state = ipc_state

                self.debug_fd_count()


            except Exception:
                reason = traceback.format_exc()
                self.safestop_controller.enter_safestop(reason=reason)             


    def debug_fd_count(self) -> None:
        try:
            count = len(os.listdir("/proc/self/fd"))
            if count > 50:
                print("WARN: OPEN_FD_COUNT =", count)
                self.log_system(f"WARN: OPEN_FD_COUNT ={count}")
        except Exception as err:
            print("FD debug failed:", err)

    def refresh_jobs_done_today_display(self):
        # in UI dash

        count = self.audit_repo.count_completed_jobs_today()
        self.ui.tk_set_jobs_done_today(count)


    def update_ui_status(self, requested_status=None) -> None:
        
        # note to self: clarify source of ui_status
        try:
            handover_data = self.handover_repo.read()
            ipc_state = handover_data.get("ipc_state")
        except Exception:
            ipc_state = None
        

        if ipc_state == "safestop":
            ui_status = "safestop"

        elif requested_status == "working" or ipc_state in ("job_queued", "job_running", "job_verifying"):
            ui_status = "working"

        elif self.network_service.network_state is False:
            ui_status = "no network"

        elif not self.is_within_operating_hours():
            ui_status = "ooo"

        else:
            ui_status = "online"

        if self.prev_ui_status != ui_status:
            self.ui.tk_set_status(ui_status)
            self.prev_ui_status = ui_status


    def log_ui_and_system(self, text:str, job_id=None, blank_line_before: bool = False) -> None:
        
        self.ui.tk_set_log(text, blank_line_before)
        
        #text = text.replace("\n", " ")
        self.log_system(text, job_id=job_id)


    def log_system(self, event_text: str, job_id=None, file="system.log"):

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # get caller function name
        try:
            frame = sys._getframe(1)
            caller_name = frame.f_code.co_name
            instance = frame.f_locals.get("self")
            if instance is not None:
                class_name = instance.__class__.__name__
                caller = f"{class_name}.{caller_name}()"
            else:
                caller = f"{caller_name}()"

        except Exception:
            caller = "unknown_caller()"

        job_part = f" | JOB {job_id}" if job_id else ""
        log_line = f"{timestamp}{job_part} | {caller} | {event_text}"

        #last_err = None
        for i in range(7):
            try:
                with open(file, "a", encoding="utf-8") as f:
                    f.write(log_line + "\n")
                    f.flush()
                return

            except Exception as err:
                #last_err = err
                print(f"WARN: retry {i+1}/7 from log_system():", err)
                time.sleep(i + 1)

        #raise RuntimeError(f"log_system() failed after 7 attempts: {last_err}")     # allow system to work w/o log?
 

    def check_for_jobs(self) -> bool:
        # mailjobs-flow is allowed to starve scheduledjobs-flow 
        
        # 1. Mail first (priority)
        mail_result = self.mail_flow.poll_once()
        if mail_result.handover_data is not None:
            self.handover_repo.write(mail_result.handover_data)
            return True
        
        if mail_result.handled_anything:  # e.g. 'jobping' that requres no handover/'front-end RPA activity'
            return True   

         # 2. Scheduled jobs
        scheduled_result = self.scheduled_flow.poll_once()
        if scheduled_result.handover_data is not None:
            self.handover_repo.write(scheduled_result.handover_data)
            return True
   

        return False


    def generate_job_id(self) -> int:
        ''' unique id for all jobs '''

        job_id = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        
        # bullet-proof dublicate-value prevention
        while self.audit_repo.get_latest_job_id() >= job_id:
            time.sleep(1)
            job_id = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

        self.log_system(f"new job_id is: {job_id}")
        return job_id

    
    def is_within_operating_hours(self) -> bool:
        
        now = datetime.datetime.now().time()
        result = datetime.time(5,0) <= now <= datetime.time(23,0) # eg. working hours 05:00 to 23:00
        
        self.log_system(f"returning: {result}")
        return result
        

    def finalize_current_job(self, handover_data) -> None:
        
        self.post_handover_filalizer.poll_once(handover_data)

        self.handover_repo.write({"ipc_state": "idle"})


    def poll_for_stop_flag(self, stopflag="stop.flag"):
        # to stop python on operator manual stop on front-end RPA

        self.log_system("poll_for_stop_flag() alive")

        while True:
            time.sleep(1)
            
            if os.path.isfile(stopflag):
                try: os.remove(stopflag)
                except Exception: pass

                try: self.log_system(f"found {stopflag}")
                except Exception: pass
                
                try: self.ui.tk_set_shutdown() #request soft-exit
                except Exception: os._exit(1)
                
                time.sleep(1)
                os._exit(0)  #kill if still alive after 1 sec 

# for dashboard - "the face"
class DashboardUI:
    def __init__(self):
        bg_color ="#000000" #or "#111827"
        text_color = "#F5F5F5"

        self._build_root(bg_color)
        self._build_header(bg_color, text_color)
        self._build_body(bg_color, text_color)
        self._build_footer(bg_color, text_color)
        
        #self.debug_grid(self.root)


    def attach_runtime(self, robot_runtime) -> None:
        self.robot_runtime = robot_runtime


    def run(self) -> None:
        self.root.mainloop()


    def _build_root(self,bg_color):
        self.root = tk.Tk()
        self.root.geometry('1800x1000+0+0')
        #self.root.geometry('1800x200+0+0')
        #self.root.attributes("-fullscreen", True)
        self.root.resizable(False, False)

        self.root.configure(bg=bg_color, padx=50)
        self._closing = False
        self.root.protocol("WM_DELETE_WINDOW", self.shutdown)

        self.root.title('RPA dashboard')
        self._create_recording_overlay()

        # layout using grid
        self.root.grid_rowconfigure(1, weight=1)
        self.root.grid_columnconfigure(0, weight=1)


    def _build_header(self, bg_color, text_color):
        self.header = tk.Frame(self.root, bg=bg_color)
        
        self.header.grid(row=0, column=0, sticky="ew")
        self.header.grid_columnconfigure(2, weight=1)  
        self.header.grid_rowconfigure(0, weight=1)  

        # Header content
        self.rpa_text_label = tk.Label(self.header, text="RPA:", fg=text_color, bg=bg_color, font=("Arial", 100, "bold"))  #snyggare: "Segoe UI"
        self.rpa_text_label.grid(row=0, column=0, padx=16, pady=16, sticky="w")
        self.rpa_status_label = tk.Label(self.header, text="", fg="red", bg=bg_color, font=("Arial", 100, "bold"))
        self.rpa_status_label.grid(row=0, column=1, padx=16, pady=16, sticky="w")
        self.status_dot = tk.Label(self.header, text="", fg="#22C55E", bg=bg_color, font=("Arial", 50, "bold"))
        self.status_dot.grid(row=0, column=2, sticky="w")


        # jobs done today (counter + label in same grid)
        self.jobs_counter_frame = tk.Frame(self.header, bg=bg_color)
        self.jobs_counter_frame.grid(row=0, column=3, sticky="ne", padx=40, pady=30)
        self.jobs_counter_frame.grid_rowconfigure(0, weight=1)
        self.jobs_counter_frame.grid_columnconfigure(0, weight=1)


        # normal view (jobs done today)
        self.jobs_normal_view = tk.Frame(self.jobs_counter_frame, bg=bg_color)
        self.jobs_normal_view.grid(row=0, column=0, sticky="nsew")
        self.jobs_normal_view.grid_columnconfigure(0, weight=1)

        self.jobs_done_label = tk.Label(    self.jobs_normal_view,    text="0",    fg=text_color,    bg=bg_color,    font=("Segoe UI", 140, "bold"),       anchor="e",        justify="right")
        self.jobs_done_label.grid(row=0, column=0, sticky="e")

        self.jobs_counter_text = tk.Label(            self.jobs_normal_view,            text="jobs done today",            fg="#A0A0A0",            bg=bg_color,            font=("Arial", 14, "bold"),            anchor="e"        )
        self.jobs_counter_text.grid(row=1, column=0, sticky="e", pady=(0, 6))

        # safestop view (big X)
        self.jobs_error_view = tk.Frame(self.jobs_counter_frame, bg=bg_color)
        self.jobs_error_view.grid(row=0, column=0, sticky="nsew")

        self.safestop_x_label = tk.Label(            self.jobs_error_view,                        text="X",            bg="#DC2626",            fg="#FFFFFF",            font=("Segoe UI", 140, "bold")        ) #text="✖",
        self.safestop_x_label.pack(expand=True)


        # show normal view at startup
        self.jobs_normal_view.tkraise()

        # 'online'-status animation
        self._online_animation_after_id = None
        self._online_pulse_index = 0

        # 'working...'-status animation
        self._working_animation_after_id = None
        self._working_dots = 0


    def _build_body(self,bg_color, text_color):
        self.body = tk.Frame(self.root, bg=bg_color)        
        self.body.grid(row=1, column=0, sticky="nsew")
        self.body.grid_rowconfigure(0, weight=1)
        self.body.grid_columnconfigure(0, weight=1)

        # body content
        log_and_scroll_container = tk.Frame(self.body, bg=bg_color)
        log_and_scroll_container.grid(row=0, column=0, sticky="nsew")
        log_and_scroll_container.grid_rowconfigure(0, weight=1)
        log_and_scroll_container.grid_columnconfigure(0, weight=1)

        # the right-hand side scrollbar
        scrollbar = tk.Scrollbar(log_and_scroll_container, width=23, troughcolor="#0F172A", bg="#1E293B", activebackground="#475569", bd=0, highlightthickness=0, relief="flat")
        scrollbar.grid(row=0, column=1, sticky="ns")

        # the 'console'-style log
        self.log_text = tk.Text(log_and_scroll_container, yscrollcommand=scrollbar.set, bg=bg_color, fg=text_color, insertbackground="black", font=("DejaVu Sans Mono", 20), wrap="none", state="disabled", bd=0,highlightthickness=0) #glow highlightbackground="#1F2937", highlightthickness=1 
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar.config(command=self.log_text.yview)


    def _build_footer(self,bg_color, text_color):
        self.footer = tk.Frame(self.root, bg=bg_color)        
        self.footer.grid(row=2, column=0, sticky="nsew")
        self.footer.grid_rowconfigure(0, weight=1)
        self.footer.grid_columnconfigure(0, weight=1)
        
        # footer content
        self.last_activity_label = tk.Label(self.footer, text="last activity: xx:xx", fg="#A0A0A0", bg=bg_color, font=("Arial", 14, "bold"), anchor="e")
        self.last_activity_label.grid(row=0, column=1, padx=8, pady=16)


    def debug_grid(self,widget):
        #highlights all grids with red
        for child in widget.winfo_children():
            try:
                child.configure(highlightbackground="red", highlightthickness=1)
            except Exception:
                pass
            self.debug_grid(child)


    def update_status_display(self, status=None):
        # sets the status

        # stops any ongoing animations
        self._stop_online_animation()
        self._stop_working_animation()
        self.status_dot.config(text="")


        # changes text
        if status=="online":
            self.rpa_status_label.config(text="online", fg="#22C55E")
            self.jobs_normal_view.tkraise()
            self.status_dot.config(text="●")
            self._start_online_animation()
            
        elif status=="no network":
            self.rpa_status_label.config(text="no network", fg="red")
            self.jobs_normal_view.tkraise()
            
        elif status=="working":
            self.rpa_status_label.config(text="working...", fg="#FACC15")
            self.jobs_normal_view.tkraise()
            self._start_working_animation()

        elif status=="safestop":
            self.rpa_status_label.config(text="safestop", fg="red")
            self.jobs_error_view.tkraise()
            
        elif status=="ooo":
            self.rpa_status_label.config(text="out-of-office", fg="#FACC15")
            self.jobs_normal_view.tkraise()


    def set_jobs_done_today(self, n) -> None:
        self.jobs_done_label.config(text=str(n))


    def _create_recording_overlay(self) -> None:
        #written by AI
        self.recording_win = tk.Toplevel(self.root)
        self.recording_win.withdraw()                 # hidden at start
        self.recording_win.overrideredirect(True)    # no title/boarder
        self.recording_win.configure(bg="black")

        try: self.recording_win.attributes("-topmost", True)
        except Exception: pass

        width = 250
        height = 110
        x = self.root.winfo_screenwidth() - width - 30
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.recording_win.geometry(f"{width}x{height}+{x}+{y}")

        frame = tk.Frame(           self.recording_win,            bg="black",            highlightbackground="#444444",            highlightthickness=1,            bd=0        )
        frame.pack(fill="both", expand=True)

        canvas = tk.Canvas(        frame,        width=44,        height=44,        bg="black",        highlightthickness=0,        bd=0        )
        canvas.place(x=18, y=33)
        canvas.create_oval(4, 4, 40, 40, fill="#DC2626", outline="#DC2626")

        label = tk.Label(            frame,            text="RECORDING",            fg="#FFFFFF",            bg="black",            font=("Arial", 20, "bold"),            anchor="w"        )
        label.place(x=75, y=33)

        
    def show_recording_overlay(self) -> None:
        #written by AI
        try:
            width = 250
            height = 110
            x = self.root.winfo_screenwidth() - width - 30
            y = (self.root.winfo_screenheight() // 2) - (height // 2)
            self.recording_win.geometry(f"{width}x{height}+{x}+{y}")

            self.recording_win.deiconify()
            self.recording_win.lift()

            try:
                self.recording_win.attributes("-topmost", True)
            except Exception:
                pass
        except Exception:
            pass


    def hide_recording_overlay(self) -> None:
        # hides recording window
        try: self.recording_win.withdraw()
        except Exception: pass


    def _start_working_animation(self):
        if self._working_animation_after_id is None:
            self._animate_working()

    def _animate_working(self):
        #written by AI
        states = ["working", "working.", "working..", "working..."]
        self._working_dots = (self._working_dots + 1) % len(states)
        self.rpa_status_label.config(text=states[self._working_dots])
        self._working_animation_after_id = self.root.after(500, self._animate_working)

    def _stop_working_animation(self):
        if self._working_animation_after_id is not None:
            self.root.after_cancel(self._working_animation_after_id)
            self._working_animation_after_id = None
            self._working_dots = 0

    def _start_online_animation(self):
        if self._online_animation_after_id is None:
            self._online_pulse_index = 0
            self._animate_online()

    def _animate_online(self):
        # green puls animation
        colors = ["#22C55E", "#16A34A","#000000", "#15803D", "#16A34A"]
        color = colors[self._online_pulse_index]

        self.status_dot.config(fg=color)

        self._online_pulse_index = (self._online_pulse_index + 1) % len(colors)
        self._online_animation_after_id = self.root.after(1000, self._animate_online)

    def _stop_online_animation(self):
        if self._online_animation_after_id is not None:
            self.root.after_cancel(self._online_animation_after_id)
            self._online_animation_after_id = None

        
    def append_ui_log(self, log_line: str, blank_line_before: bool = False) -> None:
        # appends the consol-style log

        self.log_text.config(state="normal") # open for edit
        now = datetime.datetime.now().strftime("%H:%M")

        if blank_line_before:
            self.log_text.insert("end", "\n")

        self.log_text.insert("end", f"[{now}] {log_line}\n")

        self.log_text.config(state="disabled") # closing edit
        self.log_text.see("end")


    def shutdown(self) -> Never | None:
        if self._closing: return
        self._closing = True

        try: self.robot_runtime.recording_service.stop()
        except Exception: pass

        self.root.destroy()

    # all 'tk_set_...' are wrappers
    def tk_set_status(self, status: str) -> None:
        self.root.after(0, lambda: self.update_status_display(status))

    def tk_set_log(self, text: str, blank_line_before: bool = False) -> None:
        self.root.after(0, lambda: self.append_ui_log(text, blank_line_before))

    def tk_set_show_recording_overlay(self) -> None:
        self.root.after(0, self.show_recording_overlay)

    def tk_set_hide_recording_overlay(self) -> None:
        self.root.after(0, self.hide_recording_overlay)

    def tk_set_jobs_done_today(self, n: int) -> None:
        self.root.after(0, lambda: self.set_jobs_done_today(n))
    
    def tk_set_shutdown(self,) -> None:
        self.root.after(0, self.shutdown)


def main() -> None:
    #run dashboard in main thred and 'the rest' in async worker
    ui = DashboardUI()
    robot_runtime = RobotRuntime(ui)
    ui.attach_runtime(robot_runtime)

    threading.Thread(target=robot_runtime.run, daemon=True).start() # 'the rest'
    threading.Thread(target=robot_runtime.poll_for_stop_flag, daemon=True).start() # killswitch triggered by front-end RPA stop

    ui.run()


if __name__ == "__main__":
    main()