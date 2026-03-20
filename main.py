# goal of this project is to create a beginner friendly-ish framework for automating tasks in a limited business environment.
# it could be used for piloting purpose, for you and your team of 5-10 people. its designed to run on a normal operator computer
# without administrator rights, it could be an "extra-device" dedicated computer or your normal one. simplicity over scaleability. 
# while this "back-end" framework is the orchestrator,
# it should be pared with "front-end" screen-activites like clicks and keyboard-text from a conventional RPA applikation 
# (like bluprism, power automate, uipath studio...). This combines the simplicity of python logic and the simplicity
# of external RPA screen activity.
# for lager scale application, see Robot Framework github.
# This is more of an out-of-the box framwork, including a email pipeline for email-triggered job processing and 
# a placeholder for self initiated schedule jobs.
# to run this you need only main.py, and to test it use fake_work_generator.py and conventional_rpa_simulator.py
# but first of all, check out workflow.png to understand the how communication between this runtime framework and the conventional rpa works.

#citat: "Det här är bättre än “beginner friendly-ish”. Det är faktiskt ett ganska genomtänkt litet orchestrator-ramverk för low-cost RPA på operatörsdator."

#policy:
# Efter safestop/omstart av RPA/python är det alltid ett nytt kallt startläge.
# I produktion körs RPA på en windows-laptop utan admin-rättigheter, men dev sker i ubuntu, så koden behöver funka för båda. Python ver 3.14
# att döda processer är ok, jag kommer spara en lista över OK-processnamn taget när automationen är i full gång, och döda (matchat på namn) alla andra processer för att nollställa hela datorn efter varje jobb. Det är en dedikerat RPA-dator som inte ska ha massa pop-ups osv.
# normal ipc_state får endast ändras via write(). fatal ipc_state intern nödstoppstatus i Python
# email FÅR svälta schemalagda jobb, alltid prio på email.
# koden körs på en dedikerad RPA dator utan andra uppgifter. 
# max ett email per användare med "lifesign-notice" ska skickas per dag
# ett emailsvar ska skickas med antingen job done eller job failed till användare i friends.xls
# no resume policy: unfinished, paused or crashed jobs should not resume
# an operator can manually create reboot.flag to reboot this script, if system is in state "safestop" (this enables remote bug-fixes)
# this is the most simple and cheap set-up where you, as a team-member, will request an extra device (= no additional license cost for OS, Office etc.) and make it a dedicated RPA-machine.  
#Audit-status i SQLite ska namnen beskriva jobbets livscykel: REJECTED (error by user, eg no access or invalid request), QUEUED (waiting for RPA), job_running, VERIFYING (double check with query), DONE, FAILED (error by robot, eg verification failed or crash)

#job producers: direkt inkommande mail,bevakning av gemensam mailkorg, schemalagda jobb (PersonalInboxSource,SharedMailboxSource,ScheduledJobSource)

# info about RPA:
# 1. On operator start of the external RPA-software it creates handover.txt with ipc_state "idle"
# 2. It runs this python script below async and then enters a while-true loop
# 3. within a TRY, the loop reads handover.txt and, and if read "queued", changes it to "job_running"
# 4. during 'job_running' it performs the automation in ERP, and when done changes handover.txt to "job_verifying"
# 5. any errors are catched en Except, that changes handover.txt to 'safestop'
# 6. all other states than 'queued' are ignored
# 7. On operator stop, the external RPA should create 'stop.flag' to also stop this script.  

#detta är ett litet RPA-ramverk för en vanlig operatörsdator som passar små verksamhetsnära automationer.
#ctrl + K + 2 för att collapsa alla metoder
import tkinter as tk
import time, random, threading, traceback, os, tempfile, sys, platform, subprocess, signal, atexit, sqlite3, datetime, shutil, re
from openpyxl import Workbook, load_workbook #type: ignore
from typing import Never, Literal
from pathlib import Path
from email.parser import BytesParser
from email.utils import parseaddr
from dataclasses import dataclass
from email import policy

#Alla källor ska producera samma typ av objekt, t.ex.:


'''
job_states:
    "REJECTED",        # rejected before execution (user issue)
    "QUEUED",          # job accepted and queued to external robot
    "RUNNING",         # external robot executing
    "VERIFYING",       # verifying external result with SQL if possible
    "DONE",            # success
    "FAILED",          # failed or robot/system error
'''

@dataclass
class CandidateDecision:
    action: Literal["DELETE_ONLY", "REPLY_AND_DELETE", "QUEUE_RPA_JOB", "SKIP", "MOVE_BACK_TO_INBOX", "CRASH"]
    job_type: str | None = None
    reply_subject: str | None = None
    reply_body: str | None = None
    audit_status: Literal["REJECTED", "QUEUED", "RUNNING", "VERIFYING", "DONE", "FAILED"]| None = None
    audit_error_code: str | None = None
    audit_error_explanation: str | None = None
    handover_payload: dict | None = None
    ui_message: str | None = None
    system_message: str | None = None
    send_lifesign_notice: bool = False
    start_recording: bool = False
    crash_reason: str | None = None


@dataclass
class MailItemCandidate:
    id: str
    sender_email: str
    sender_name: str
    subject: str
    body: str
    headers: dict[str, str]
    source_ref: str | Path | None = None  # in dev: Path     # in Outlook : message id 
    job_source_type: Literal["own_inbox", "shared_inbox"] | None=None


@dataclass
class ScheduledJobsItemCandidate:
    order_number: int
    order_qty: int
    material_available: int
    job_source_type: Literal["fake_db"] | None=None


@dataclass
class FlowResult:
    handled_anything: bool
    handover_data: dict | None = None


# for external email app (remake to fit eg. OutlookMailBackend) byt namn till DevFile...
class DevFileMailBackend:
    def __init__(self, append_system_log, pipeline_root) -> None:
        self.append_system_log = append_system_log
        self.pipeline_root = Path(pipeline_root) # change to folder in e.g. outlook
        self.inbox_dir = self.pipeline_root / "inbox"
        self.processing_dir = self.pipeline_root / "processing"

        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        self.processing_dir.mkdir(parents=True, exist_ok=True)


    def fetch_next_from_inbox(self) -> Path | None:
        email_files = sorted(self.inbox_dir.glob("*.eml"))
        if not email_files:
            return None

        self.append_system_log(f"found personal inbox email: {email_files[0]}")
        return email_files[0]

    def fetch_all_from_inbox(self) -> list | None:
        email_files = sorted(self.inbox_dir.glob("*.eml"))
        if not email_files:
            return None
        
        self.append_system_log(f"found one or many shared inbox email: {email_files[0]}")

        return email_files


    def claim_to_processing(self, inbox_path: Path) -> Path:
        target_path = self.processing_dir / inbox_path.name
        self.append_system_log(f"moving {inbox_path} to {target_path}")
        shutil.move(str(inbox_path), str(target_path))
        return target_path

    def parse_processing_mail(self, processing_path: Path) -> MailItemCandidate:
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

        return MailItemCandidate(
            id=message_id,
            sender_email=from_address.strip().lower(),
            sender_name=from_name.strip(),
            subject=subject,
            body=body,
            headers=headers,
            source_ref=processing_path,
        )

    def reply_and_delete(self, mail: MailItemCandidate, subject: str, body: str, job_id: int | None = None) -> None:
        self.send_reply(mail, subject, body, job_id)
        self.delete_from_processing(mail, job_id)

    def send_reply(self, mail: MailItemCandidate, subject: str, body: str, job_id: int | None = None) -> None:
        # DEV STUB
        self.append_system_log(
            f"reply stub to={mail.sender_email} subject={subject!r} body={body[:120]!r}",
            job_id,
        )
        print(
            f"##########\nreply stub to={mail.sender_email} \nsubject={subject!r} \nbody={body} \njob_id={job_id}\n##############",
        )

    def delete_from_processing(self, mail: MailItemCandidate, job_id: int | None = None) -> None:
        if not isinstance(mail.source_ref, Path):
            raise ValueError("delete_from_processing() expected Path source_ref in dev mode")

        self.append_system_log(f"removing: {mail.source_ref}", job_id)
        os.remove(mail.source_ref)

    def move_back_to_inbox(self, mail: MailItemCandidate) -> None:
        #stub
        pass




class MailMaster:
    def __init__(self, in_dev_mode, append_system_log, append_ui_and_system_log, friends_repo, is_within_operating_hours, network_service, recording_service, generate_job_id, audit_repo, update_ui_status, refresh_jobs_done_today_display, post_show_recording_overlay, job_handlers, safestop_controller) -> None:
        self.in_dev_mode = in_dev_mode
        self.append_system_log = append_system_log
        self.append_ui_and_system_log = append_ui_and_system_log
        self.friends_repo = friends_repo
        self.is_within_operating_hours = is_within_operating_hours
        self.network_service = network_service
        self.recording_service = recording_service
        self.generate_job_id = generate_job_id
        self.audit_repo = audit_repo
        self.update_ui_status = update_ui_status
        self.refresh_jobs_done_today_display = refresh_jobs_done_today_display
        self.ui_dot_post_show_recording_overlay = post_show_recording_overlay
        self.safestop_controller = safestop_controller

        self.mail_backend_own = DevFileMailBackend(self.append_system_log, pipeline_root="own_inbox",  )
        self.mail_backend_shared = DevFileMailBackend(self.append_system_log, pipeline_root="shared_inbox",  )

        self.job_handlers = job_handlers

    def process_one_cycle(self) -> FlowResult:
        
        #candidate = all mail from own inbox, 'is_shared_inbox_mail_in_scope'-mail from shared
        candidate = self.fetch_next_claimed_and_parsed_candidate_from_own_and_shared_inboxes()
        if not candidate:
            return FlowResult(handled_anything=False, handover_data=None)
        
        elif candidate.job_source_type == "own_inbox":
            if self.friends_repo.reload_if_changed():
                self.append_ui_and_system_log("friends.xlsx reloaded", blank_line_before=True)

            self.append_ui_and_system_log(f"email from {candidate.sender_email}", blank_line_before=True)
            decision = self.make_decision_own_inbox(candidate)

    
        elif candidate.job_source_type == "shared_inbox":
            decision = self.make_decision_shared_inbox(candidate)
            

        handover_data = self.execute_decision(candidate, decision) #same?
        return FlowResult(handled_anything=True, handover_data=handover_data)


    def fetch_next_claimed_and_parsed_candidate_from_own_and_shared_inboxes(self) -> MailItemCandidate | None:

        #fetch from 
        inbox_path = self.mail_backend_own.fetch_next_from_inbox()
        if inbox_path:
            processing_path = self.mail_backend_own.claim_to_processing(inbox_path)
            mail = self.mail_backend_own.parse_processing_mail(processing_path)
            del inbox_path
            mail.job_source_type="own_inbox"
            self.append_system_log(f"{mail.job_source_type} produced mail {mail.id}")
            return mail

        else:
            list_of_inbox_path = self.mail_backend_shared.fetch_all_from_inbox()
            if not list_of_inbox_path:
                return None
            
            for inbox_path in list_of_inbox_path:
                mail = self.mail_backend_shared.parse_processing_mail(inbox_path)
                if not self.is_shared_inbox_mail_in_scope(mail):
                    continue

                processing_path = self.mail_backend_shared.claim_to_processing(inbox_path)
                mail.job_source_type="shared_inbox"
                self.append_system_log(f"{mail.job_source_type} produced mail {mail.id}")

                return mail

        return None


    def is_shared_inbox_mail_in_scope(self,mail):
        #placeholder for checks
        self.append_system_log(f"placeholder scope check for {mail}")
        return True

    
    def classify_mail_own_inbox(self, mail: MailItemCandidate) -> str:
        subject = mail.subject.strip().lower()

        if subject.startswith("ping"):
            return "ping"
        if "job1" in subject:
            return "job1"
        if "job2" in subject:
            return "job2"

        return "unknown"


    def classify_mail_shared_inbox(self):
        #subb
        pass
 

    def make_decision_own_inbox(self, mail: MailItemCandidate) -> CandidateDecision:
        job_type = None

        try:
            if not self.friends_repo.is_allowed_sender(mail.sender_email):
                return CandidateDecision(
                    action="DELETE_ONLY",
                    ui_message="--> rejected (not in friends.xlsx)",
                )

            if not self.is_within_operating_hours():
                return CandidateDecision(
                    action="REPLY_AND_DELETE",
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body="Email received outside working hours 05-23.",
                    audit_status="REJECTED",
                    audit_error_code="OUTSIDE_WORKING_HOURS",
                    ui_message="--> rejected (outside working hours)",
                )

            job_type = self.classify_mail_own_inbox(mail)

            if job_type == "unknown":
                return CandidateDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body="Could not identify a job type.",
                    audit_status="REJECTED",
                    audit_error_code="UNKNOWN_JOB",
                    ui_message="--> rejected (unable to identify job type)",
                )

            if not self.friends_repo.has_job_access(mail.sender_email, job_type):
                return CandidateDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body=f"No access to {job_type}. Check with administrator for access.",
                    audit_status="REJECTED",
                    audit_error_code="NO_ACCESS",
                    ui_message=f"--> rejected (no access to {job_type})",
                )



            if not self.network_service.has_network_access():
                return CandidateDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body="No network connection. Your email was removed.",
                    audit_status="REJECTED",
                    audit_error_code="NO_NETWORK",
                    ui_message="--> rejected (no network connection)",
                )

            handler = self.job_handlers.get(job_type)
            if handler is None:
                return CandidateDecision(
                    action="CRASH",
                    job_type=job_type,
                    crash_reason=f"No handler registered for job_type={job_type}",
                )

            ok, payload_or_error = handler.precheck_data_and_files(mail)
            if not ok:
                error = str(payload_or_error)
                return CandidateDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"FAIL re: {mail.subject}",
                    reply_body=error, #error message from precheck...()
                    audit_status="REJECTED",
                    audit_error_code="INVALID_INPUT",
                    audit_error_explanation=error,
                    ui_message=f"--> rejected (invalid input for {job_type})",
                )
            

            if job_type == "ping":
                handler.play_sound()
                return CandidateDecision(
                    action="REPLY_AND_DELETE",
                    job_type=job_type,
                    reply_subject=f"DONE re: {mail.subject}",
                    reply_body="PONG (robot online).",
                    audit_status="DONE",
                    ui_message="--> done (ping)",
                )
            
            payload = payload_or_error

            return CandidateDecision(
                action="QUEUE_RPA_JOB",
                job_type=job_type,
                audit_status="QUEUED",
                system_message=f"accepted ({job_type})",
                send_lifesign_notice=True,
                start_recording=True,
                handover_payload={
                    "job_type": job_type,
                    "email_id": mail.id,
                    **payload,
                },
            )

        except Exception as err:
            return CandidateDecision(
                action="CRASH",
                job_type=job_type,
                crash_reason=str(err),
            )
    
 
    def make_decision_shared_inbox(self, mail: MailItemCandidate) -> CandidateDecision:
        #stub
        return CandidateDecision(
                    action="MOVE_BACK_TO_INBOX",
                    system_message=f"No logic yet, move back this email to inbox from proccessing-folder: {mail.sender_email}" #only in DEV
                )


    def execute_decision(self, mail: MailItemCandidate, decision: CandidateDecision) -> dict | None:
        if decision.ui_message:
            self.append_ui_and_system_log(decision.ui_message)

        if decision.system_message:
            self.append_system_log(decision.system_message)

        if decision.action == "MOVE_BACK_TO_INBOX": # eg. if somethings wrong with in-scoop emails from shared inbox
            self.get_mail_backend(mail).move_back_to_inbox(mail)
            return None

        if decision.action == "DELETE_ONLY":
            self.get_mail_backend(mail).delete_from_processing(mail) 
            return None

        if decision.action == "REPLY_AND_DELETE":
            if decision.reply_subject is None or decision.reply_body is None:
                raise ValueError("action REPLY_AND_DELETE requires reply_subject and reply_body")
            job_id = self.generate_job_id()

            now = datetime.datetime.now()
            self.audit_repo.insert_into_db(
                job_id=job_id,
                email_address=mail.sender_email,
                email_subject=mail.subject,
                job_type=decision.job_type,
                job_start_date=now.strftime("%Y-%m-%d"),
                job_start_time=now.strftime("%H:%M:%S"),
                job_finish_time=now.strftime("%H:%M:%S"),
                job_status=decision.audit_status,
                error_code=decision.audit_error_code,
                error_explanation=decision.audit_error_explanation,
            )
            #if the job could be DONE without handover, e.g. "ping"
            if decision.audit_status == "DONE":
                self.refresh_jobs_done_today_display()

            self.get_mail_backend(mail).reply_and_delete(
                mail,
                subject=decision.reply_subject,
                body=decision.reply_body,
                job_id=job_id,
            )
 
            return None

        if decision.action == "QUEUE_RPA_JOB":
            job_id = self.generate_job_id()
            now = datetime.datetime.now()

            self.update_ui_status("working")

            
            # send lifesign notice only once a day per user
            if decision.send_lifesign_notice and not self.audit_repo.has_sender_job_today(mail.sender_email):
                self.get_mail_backend(mail).send_reply(
                    mail=mail,
                    subject = f"ONLINE re: {mail.subject}",
                    body = (">HELLO HUMAN\n\n"
                    "This is an automated system reply.\n\n"
                    "It appears to be your first request today, so this reply confirms that the robot is online.\n"
                    "Your job has been received and is now processing.\n"
                    "You will receive another message when the job is completed."),
                    job_id=job_id,
                )

            
            self.audit_repo.insert_into_db(
                job_id=job_id,
                email_address=mail.sender_email,
                email_subject=mail.subject,
                job_type=decision.job_type,
                job_start_date=now.strftime("%Y-%m-%d"),
                job_start_time=now.strftime("%H:%M:%S"),
                job_status="QUEUED",
            )

            if decision.start_recording:
                if not self.in_dev_mode:
                    self.recording_service.start(job_id)
                self.ui_dot_post_show_recording_overlay()



            if decision.handover_payload is None:
                raise RuntimeError("handover_payload is None for QUEUE_RPA_JOB")

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
                self.audit_repo.insert_into_db(
                    job_id=job_id,
                    email_address=mail.sender_email,
                    email_subject=mail.subject,
                    job_type=decision.job_type,
                    job_start_date=now.strftime("%Y-%m-%d"),
                    job_start_time=now.strftime("%H:%M:%S"),
                    job_finish_time=now.strftime("%H:%M:%S"),
                    job_status="FAILED",
                    error_code="SYSTEM_CRASH",
                    error_explanation=decision.crash_reason,
                )
            except Exception:
                pass

            try:
                self.get_mail_backend(mail).reply_and_delete(
                    mail,
                    subject=f"FAILED re: {mail.subject}",
                    body="System crash, the robot is now out-of-service and your email was deleted.",
                    job_id=job_id,
                )
            except Exception:
                pass

            self.append_ui_and_system_log("--> rejected (system crash)")
            self.safestop_controller.enter_safestop(reason=decision.crash_reason, job_id=job_id)
            return None

        raise ValueError(f"unknown decision.action={decision.action}")

    def get_mail_backend(self, mail: MailItemCandidate) -> DevFileMailBackend:
        if mail.job_source_type == "own_inbox":
            return self.mail_backend_own
        if mail.job_source_type == "shared_inbox":
            return self.mail_backend_shared
        raise ValueError(f"unknown job_source_type={mail.job_source_type}")
    

    def send_final_job_reply(self, job_id, status) -> None:
        # senare: bygg på audit.db + riktigt mailsystem
        pass



class DevERPQueryBackend:
    def __init__(self) -> None:
        pass

    def DEV_select_all_fom_db(self, path="table_in_ERP.xlsx") -> list:
        # this funktion the return from a no-extra-garbage query

        self.DEV_ensure_fake_db_exists()

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
            
        return all_rows
    
    
    def DEV_parse_row(self, row) -> ScheduledJobsItemCandidate:
              
        order_number = row.get("order_number")
        order_qty = row.get("order_qty")
        material_available = row.get("material_available")

        return ScheduledJobsItemCandidate(
            order_number=order_number,
            order_qty=order_qty,
            material_available=material_available,
        )

    
    def DEV_ensure_fake_db_exists(self, path="table_in_ERP.xlsx") -> None:
        """Makes an example file if friends.xlsx is missing."""
        if os.path.exists(path):
            return

        wb = Workbook()
        ws = wb.active
        assert ws is not None

        # headers
        ws["A1"] = "order_number"
        ws["B1"] = "order_qty"
        ws["C1"] = "material_available"


        # rows
        ws["A2"] = "10243346"
        ws["B2"] = "3200"
        ws["C2"] = "3156"
        

        ws["A3"] = "10254432"
        ws["B3"] = "7600"
        ws["C3"] = "7561"

        
        #ws["A4"] = "10256678"
        #ws["B4"] = "1212"
        #ws["C4"] = "1212"

        wb.save(path)

    def get_order_row(self, order_number) -> dict:
        # stub
        row = {"order_number": order_number, "order_qty": 3156, "material_available": 3156}
        return row


# for scheduled jobs
class ScheduledJobsMaster:
    ''' scheduled jobs pipeline '''
    def __init__(self, append_system_log, append_ui_and_system_log, update_ui_status, generate_job_id, is_within_operating_hours, network_service, recording_service, handover_repo, audit_repo, ui_dot_post_show_recording_overlay, safestop_controller, job_handlers, in_dev_mode) -> None:
        self.in_dev_mode = in_dev_mode
        
        self.append_system_log = append_system_log
        self.append_ui_and_system_log = append_ui_and_system_log
        self.update_ui_status = update_ui_status
        self.generate_job_id = generate_job_id
        self.is_within_operating_hours = is_within_operating_hours
        self.network_service = network_service
        self.recording_service = recording_service
        self.handover_repo = handover_repo
        self.audit_repo = audit_repo
        self.ui_dot_post_show_recording_overlay = ui_dot_post_show_recording_overlay
        self.safestop_controller = safestop_controller
       
        self.next_job3_check_time = 0
        self.next_job4_check_time = 0

        self.erp_query_backend = DevERPQueryBackend()
        self.job_handlers = job_handlers


    # used to handle periodic checks for (non-email) jobs, eg. new rows in a DB or new files in a folder.
    def process_one_cycle(self) -> FlowResult:
               #candidate = 
        candidate = self.fetch_next_parsed_candidate_from_all_ERP_sources()
        if not candidate:
            return FlowResult(handled_anything=False, handover_data=None)


        self.append_ui_and_system_log(f"row found order_number: {candidate.order_number}", blank_line_before=True)
        decision = self.make_decision(candidate)

        
        handover_data = self.execute_decision(candidate, decision) #same?
        return FlowResult(handled_anything=True, handover_data=handover_data)


    def fetch_next_parsed_candidate_from_all_ERP_sources(self) -> ScheduledJobsItemCandidate | None:
        self.append_system_log("running")

        #fetch from 
        all_selected_rows = self.erp_query_backend.DEV_select_all_fom_db()
   
        for row_candidate_raw in all_selected_rows:
            row_candidate = self.erp_query_backend.DEV_parse_row(row_candidate_raw)

            if self.audit_repo.has_been_worked_on_today(row_candidate.order_number):
                continue

            row_candidate.job_source_type="fake_db"
            self.append_system_log(f"{row_candidate.job_source_type} produced order_number {row_candidate.order_number}")
            return row_candidate
        
        return None


    def make_decision(self, candidate_row: ScheduledJobsItemCandidate) -> CandidateDecision:
        self.append_system_log("running")

        job_type = None

        try:

                        #placeholder evaluation logic
                        # eg. below:


            job_type = self.classify_row(candidate_row)
            handler = self.job_handlers.get(job_type)

            if candidate_row.material_available < 100:
                return CandidateDecision(
                    action="SKIP",
                    job_type=job_type,
                    audit_status="REJECTED",
                    audit_error_explanation="too few material available, manual check required",
                    ui_message=f"--> rejected (manual check required for {job_type})",
                )
            

            if handler is None:
                return CandidateDecision(
                    action="CRASH",
                    job_type=job_type,
                    crash_reason=f"No handler registered for job_type={job_type}",
                )

            ok, payload_or_error = handler.precheck_data_and_files(candidate_row)

            if not ok:
                error = str(payload_or_error)
                return CandidateDecision(
                    action="SKIP",  
                    job_type=job_type,
                    audit_status="REJECTED",
                    audit_error_code="INVALID_INPUT",
                    audit_error_explanation=error,
                    ui_message=f"--> rejected (invalid input for {job_type})",
                )

            payload = payload_or_error
            
            
            return CandidateDecision(
                action="QUEUE_RPA_JOB",
                job_type=job_type,
                audit_status="QUEUED",
                system_message=f"accepted ({job_type})",
                start_recording=True,
                handover_payload={
                    "job_type": job_type,
                    "order_number": str(candidate_row.order_number),
                    "material_available": str(candidate_row.material_available),
                    "expected_action": "sync_qty_to_material_available",
                    "job_source_type": "fake_db",
                    **payload,
                },
                )

        except Exception as err:
            return CandidateDecision(
                action="CRASH",
                job_type=job_type,
                crash_reason=str(err),
            )
    

    def classify_row(self, row: ScheduledJobsItemCandidate) -> str:
        self.append_system_log("running")
        
        del row

        return "job3"
        #if random.randint(0,1) == 0:
        #    return "job3"
        #else:
        #    return "job4"


    def execute_decision(self, row: ScheduledJobsItemCandidate, decision: CandidateDecision) -> dict | None:
        self.append_system_log("running")

        if decision.ui_message:
            self.append_ui_and_system_log(decision.ui_message)

        if decision.system_message:
            self.append_system_log(decision.system_message)

        if decision.action == "SKIP":

            job_id = self.generate_job_id()
            now = datetime.datetime.now()

            self.audit_repo.insert_into_db(
                job_id=job_id,
                order_number=row.order_number,
                job_type=decision.job_type,
                job_start_date=now.strftime("%Y-%m-%d"),
                job_start_time=now.strftime("%H:%M:%S"),
                job_finish_time=now.strftime("%H:%M:%S"),
                job_status=decision.audit_status,
                error_code=decision.audit_error_code,
                error_explanation=decision.audit_error_explanation,
            )
            return None

        

        if decision.action == "QUEUE_RPA_JOB":
            job_id = self.generate_job_id()
            now = datetime.datetime.now()

            self.update_ui_status("working")

            self.audit_repo.insert_into_db(
                job_id=job_id,
                order_number=row.order_number,
                job_type=decision.job_type,
                job_start_date=now.strftime("%Y-%m-%d"),
                job_start_time=now.strftime("%H:%M:%S"),
                job_status="QUEUED",
            )

            if decision.start_recording:
                if not self.in_dev_mode:
                    self.recording_service.start(job_id)
                self.ui_dot_post_show_recording_overlay()


            if decision.handover_payload is None:
                raise RuntimeError("handover_payload is None for QUEUE_RPA_JOB")

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
                self.audit_repo.insert_into_db(
                    job_id=job_id,
                    order_number=row.order_number,
                    job_type=decision.job_type,
                    job_start_date=now.strftime("%Y-%m-%d"),
                    job_start_time=now.strftime("%H:%M:%S"),
                    job_finish_time=now.strftime("%H:%M:%S"),
                    job_status="FAILED",
                    error_code="SYSTEM_CRASH",
                    error_explanation=decision.crash_reason,
                )
            except Exception:
                pass

            self.append_ui_and_system_log("--> rejected (system crash)")
            self.safestop_controller.enter_safestop(reason=decision.crash_reason, job_id=job_id)
            return None

        raise ValueError(f"unknown decision.action={decision.action}")



# for file-IPC
class HandoverRepository:
    ''' handles handover.txt which is the communication link between this script and the external RPA  '''

    VALID_JOB_TYPES = ("ping", "job1", "job2", "job3", "job4")
    VALID_SYSTEM_STATES = ("idle", "job_queued", "job_running", "job_verifying", "safestop")
    
    def __init__(self, append_system_log) -> None:
        self.append_system_log = append_system_log

   
    def read(self) -> dict:
        last_err=None

        for attempt in range(7):
            try:
                handover_data = {}
                with open("handover.txt", "r", encoding="utf-8") as f:
                    for row in f:
                        row = row.strip()
                        if not row: continue
                        if "=" not in row: raise ValueError(f"Invalid row in handover: {row}")
                        key, value = row.split("=", 1)
                        handover_data[key.strip()] = value.strip()

                self.validate_handover_data(handover_data)
                return handover_data

            except Exception as err:
                last_err = err
                print(f"WARN: retry {attempt+1}/7 : {err}")
                time.sleep((attempt+1) ** 2)


        raise RuntimeError(f"handover.txt unreadable: {last_err}")
    
      
    def write(self, handover_data: dict) -> None:
        """ atomic write of handover.txt"""

        self.validate_handover_data(handover_data)

        for attempt in range(7):
            temp_path = None
            try:
                dir_path = os.path.dirname(os.path.abspath("handover.txt"))
                fd, temp_path = tempfile.mkstemp(dir=dir_path)    # create temp file

                #atomic write
                with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                    for key, value in handover_data.items():
                        if value is None: value = ""
                        tmp.write(f"{key}={value}\n")
                    tmp.flush()
                    os.fsync(tmp.fileno())

                os.replace(temp_path, "handover.txt") # replace original file
                self.append_system_log(f"written: {handover_data}", job_id=handover_data.get("job_id"))
                return

            except Exception as err:
                last_err = err
                print(f"{attempt+1}st warning from write()")
                self.append_system_log(f"WARN: {attempt+1}/7 error", job_id=handover_data.get("job_id"))
                time.sleep((attempt + 1) ** 2) # 1 4 9 ... 49sec

            finally: #remove temp-file if writing fails.
                if temp_path and os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except Exception: pass

        self.append_system_log(f"CRITICAL: cannot write handover.txt {last_err}", job_id=handover_data.get("job_id"))
        raise RuntimeError("CRITICAL: cannot write handover.txt")
  

    def validate_handover_data(self, handover_data) -> None:

        ipc_state = handover_data.get("ipc_state")
        job_id = handover_data.get("job_id")
        job_type = handover_data.get("job_type")


        if ipc_state not in self.VALID_SYSTEM_STATES:
            raise ValueError(f"unknown state: {ipc_state}")
        
        elif ipc_state in ("job_queued", "job_running", "job_verifying"):
            if not job_id:
                raise ValueError(f"job_id missing for {ipc_state}")
            if not job_type:
                raise ValueError(f"job_type missing for {ipc_state}")
            if job_type not in self.VALID_JOB_TYPES:
                raise ValueError(f"unkown job_type: {job_type} for {ipc_state}")
             
# for screen-recording
class RecordingService:
    """ this handles the screen-recording to cature all external RPA screen-activity """


    def __init__(self, append_system_log,) -> None:
        self.RECORDINGS_IN_PROGRESS_FOLDER = "recordings_in_progress"
        self.RECORDINGS_DESTINATION_FOLDER = "recordings_destination"

        self.append_system_log = append_system_log
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
        self.append_system_log("recording started", job_id)
  
    #stop recording
    def stop(self, job_id=None) -> None:
        #written by AI
        try:
            self.append_system_log("stop recording", job_id)
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
        # re-write to upload all files from local folder, to include previous failed uploads? (in innit?)
    
        local_file = f"{self.RECORDINGS_IN_PROGRESS_FOLDER}/{job_id}.mkv"
        local_file = Path(local_file)

        remote_path = Path(self.RECORDINGS_DESTINATION_FOLDER) / f"{job_id}.mkv"
        remote_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(max_attempts):
            try:
                
                shutil.copy2(local_file, remote_path)
                #print(f"✓ Upload successful: {remote_path}")
                self.append_system_log(f"upload success: {remote_path}", job_id)
                try: os.remove(local_file)
                except Exception: pass

                return True

            except Exception as e:
                wait_time = (attempt + 1) ** 2
                print(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
                time.sleep(wait_time)
        
        self.append_system_log(f"upload failed: {remote_path}", job_id)
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
                    self.append_system_log(f"cleanup upload of {job_id}")
                except Exception as err:
                    self.append_system_log(f"cleanup failed for {job_id}: {err}")

# for access
class FriendsRepository:
    ''' friends.xlsx is the list of users allowed to use the email to communicate with the robot '''
    def __init__(self, append_system_log) -> None:
        self.append_system_log = append_system_log
        self.friends_access = {}
        self.friends_file_mtime = None


    def ensure_friends_file_exists(self, path="friends.xlsx") -> None:
        """Makes an example file if friends.xlsx is missing."""
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
    

    def read_access_file(self, filepath="friends.xlsx") -> dict:
        #code written by AI
        """
        Reads friends.xlsx and returns eg.:

        {
            "alice@example.com": {"ping"},
            "ex2@whatever.com": {"ping", "job1"}
        }

        Presumptions:
        A1 = email
        row 1 contains job_type
        'x' gives access
    
        """
        wb = load_workbook(filepath, data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True)) # type: ignore
        if len(rows) < 2:
            raise ValueError("friends.xlsx contains no users")

        header = rows[0]   # första raden

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


    def reload_if_changed(self, force_reload=False, filepath="friends.xlsx") -> bool:
        #code written by AI
        """
        Laddar om friends.xlsx om filen ändrats sedan sist.
        force_reload=True tvingar omladdning.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"{filepath} not found")

        current_mtime = os.path.getmtime(filepath)

        if (not force_reload) and (self.friends_file_mtime == current_mtime):
            return False   # ingen ändring

        new_access = self.read_access_file(filepath)

        self.friends_access = new_access
        self.friends_file_mtime = current_mtime

        return True


    def is_allowed_sender(self, email_address: str) -> bool:
        email_address = email_address.strip().lower()
        result = email_address in self.friends_access
        self.append_system_log(f"returning: {result}")
        return result


    def has_job_access(self, email_address: str, job_type: str) -> bool:
        email_address = email_address.strip().lower()
        job_type = job_type.strip().lower()
        result = job_type in self.friends_access.get(email_address, set())
        self.append_system_log(f"returning: {result}")
        return result

# for network-check
class NetworkService:
    ''' checks if the compuster is connected to company LAN '''
    NETWORK_TEST_PATH = r"/" #enter path to network drive here, e.g. "G:\"

    def __init__(self, append_system_log) -> None:
        self.append_system_log = append_system_log
        self.network_state = False #assume offline at start
        self.next_network_check_time = 0


    def has_network_access(self) -> bool:
        #this runs at highest every hour, or before new jobs

        now = time.time()

        if now < self.next_network_check_time:
            return self.network_state

        try:
            os.listdir(self.NETWORK_TEST_PATH)
            online = True
            
        except Exception:
            online = False
            

        # logga / uppdatera UI bara vid förändring
        if online != self.network_state:
            self.network_state = online

            if online:
                self.append_system_log("network restored")
            else:
                self.append_system_log(f"WARN: network lost")

        # olika pollingintervall beroende på status
        if online:
            self.next_network_check_time = now + 3600   # 1 h
        else:
            self.next_network_check_time = now + 60     # 1 min
        
        return online

# for SQLite
class AuditRepository:
    ''' handles audit.db, that shows all jobs in a audit-style manner '''
    def __init__(self, append_system_log) -> None:
        self.append_system_log = append_system_log
        

    def create_db_if_needed(self) -> None:
        
        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()
           
            cur.execute("""
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
                        error_explanation TEXT 
                        )
                        """)


    def update_db(self, job_id, email_address=None, email_subject=None, order_number=None, job_type=None, job_start_date=None, job_start_time=None, job_finish_time=None, job_status=None, final_reply_sent=None, error_code=None,error_explanation=None,) -> None:
        # example use: self.audit_repo.update_db(job_id=20260311124501, job_type="job1")

        if job_status not in ("REJECTED", "QUEUED", "RUNNING", "VERIFYING", "DONE", "FAILED", None):
            raise ValueError(f"update_db(): unknown job_status={job_status}")

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
            "error_explanation": error_explanation,
        }

        fields = {k: v for k, v in all_fields.items() if v is not None}
        self.append_system_log(f"received fields: {fields}", job_id=job_id)

        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()

            fields.pop("job_id", None)

            if not fields:
                return

            set_clause = ", ".join(f"{k}=?" for k in fields)

            cur.execute(
                f"UPDATE audit_log SET {set_clause} WHERE job_id=?",
                (*fields.values(), job_id)
            )

            if cur.rowcount == 0:
                raise ValueError(f"update_db(): no row in DB with job_id={job_id}")


    def insert_into_db(self, job_id, email_address=None, email_subject=None, order_number=None, job_type=None, job_start_date=None, job_start_time=None, job_finish_time=None, job_status=None, final_reply_sent=None, error_code=None,error_explanation=None,) -> None:
        if job_status not in ("REJECTED", "QUEUED", "RUNNING", "VERIFYING", "DONE", "FAILED", None):
            raise ValueError(f"update_db(): unknown job_status={job_status} for INSERT INTO")

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
            "error_explanation": error_explanation,
        }

        fields = {k: v for k, v in all_fields.items() if v is not None}
        self.append_system_log(f"received fields: {fields}", job_id=job_id)

        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()

            columns = ", ".join(fields.keys())
            placeholders = ", ".join("?" for _ in fields)

            cur.execute(
                f"INSERT INTO audit_log ({columns}) VALUES ({placeholders})",
                tuple(fields.values())
            )

        

    def count_completed_jobs_today(self) -> int:

        today = datetime.date.today().isoformat()

        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*)
                FROM audit_log
                WHERE job_start_date = ?
                AND job_status = 'DONE'
            """, (today,))
            
            result = cur.fetchone()[0]
        return result

    # used to send max one notification-response a day
    def has_sender_job_today(self, email_address) -> bool:    
        

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT COUNT(*)
                FROM audit_log
                WHERE job_start_date = ? AND email_address = ?
                """,
                (today, email_address,)
            )

            jobs_today = cur.fetchone()[0]
        
        self.append_system_log(f"returning: {jobs_today > 0}")

        return jobs_today > 0


    def has_been_worked_on_today(self, order_number) -> bool:

        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT COUNT(*)
                FROM audit_log
                WHERE job_start_date = ? AND order_number = ?
                """,
                (today, order_number,)
            )

            jobs_today = cur.fetchone()[0]
        
        result = jobs_today > 0
        self.append_system_log(f"return for {order_number} is: {result}")
        return result


    # used to avoid conflicting job_id
    def get_most_recent_job(self) -> int:
        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT job_id
                FROM audit_log
                ORDER BY job_id DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            return row[0] if row is not None else 0

    # (not implemented) Failed jobs
    def get_failed_jobs(self, days=7):
        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT job_id, email_address, job_type, error_code, error_explanation
                FROM audit_log
                WHERE job_status = 'FAILED'
                AND job_start_date >= date('now', '-' || ? || ' days')
                ORDER BY job_id DESC
            """, (days,))
            return cur.fetchall()


    def has_unreplied_finished_jobs(self) -> bool:
        with sqlite3.connect("audit.db") as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT job_id
                FROM audit_log
                WHERE final_reply_sent = 0
                AND job_status IN ('DONE', 'FAILED', 'REJECTED')
                LIMIT 1
                """)
            return cur.fetchone() is not None

# for decision on incoming emails


class JobPingHandler:
    def __init__(self) -> None:
        pass
    
    def precheck_data_and_files(self, mail: MailItemCandidate) -> tuple[bool, dict | str]:
        return True, {}
    
    def play_sound(self) -> None:
        
        system = platform.system()

        if system == "Windows":
            import winsound
            # frequency (Hz), duration (ms)
            winsound.Beep(1000, 300) #type: ignore

        elif system == "Linux":
            print("\a", end="", flush=True)


# for job1-template
class Job1Handler:
    ''' this class for everything concering "job1" (except verification?) '''
    def __init__(self) -> None:
        pass

    # sanity-check on the given data, eg. are all fields supplied and in correct format?


    def precheck_data_and_files(self, mail: MailItemCandidate) -> tuple[bool, dict | str]:
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
    

    def verify_result(self, handover_data):
        return "ok"

# job2...
class Job2Handler:
    def __init__(self) -> None:
        pass
    
    def precheck_data_and_files(self, mail: MailItemCandidate) -> tuple[bool, dict | str]:
        return False, "Missing required fields for job2."

    def verify_result(self, handover_data):
        return "ok"
    
# job3...
class Job3Handler:
    ''' job3 '''
    def __init__(self) -> None:
        self.erp_query_backend = DevERPQueryBackend()

    #see job1
    def precheck_data_and_files(self, row: ScheduledJobsItemCandidate) -> tuple[bool, dict | str]:
        result = {"stub": "stub."}
        return True, result
    
    #see job1
    def do_query_to_erp(self) -> None:
        pass

    def verify_result(self, handover_data) -> str:
        order_number = handover_data.get("order_number")
        if not order_number:
            return "missing order_number"

        row = self.erp_query_backend.get_order_row(order_number)
        if row is None:
            return f"order {order_number} not found"

        if row["order_qty"] != row["material_available"]:
            return "ERP still shows mismatch after RPA update"

        return "ok"


# for asking an AI LLM
class AIHelper:
    # not completed. backend needed
    def prompt(self, input: str, question: str) -> str:

        header = "You are an agent in an RPA application."
        f"This is the file, in str format, that the question regards: {input}."
        f"The question is: {question}."

        ai_reply = "dummy"
        return ai_reply


# for crash-mode
class SafeStopController:
    def __init__(self, append_system_log, append_ui_and_system_log, recording_service, ui, mail_backend, audit_repo, generate_job_id, friends_repo) -> None:
        self.append_system_log = append_system_log
        self.append_ui_and_system_log = append_ui_and_system_log
        self.recording_service = recording_service
        self.ui = ui
        self.mail_backend = mail_backend
        self.audit_repo = audit_repo
        self.generate_job_id = generate_job_id
        self.friends_repo = friends_repo
        self._safestop_entered = False


    def enter_safestop(self, reason, job_id=None) -> None:
        #critical crashes end up here
        
        if self._safestop_entered: return #re-entrancy protection
        self._safestop_entered = True 

        print("ROBOTRUNTIME CRASHED:\n", reason)        

        self.append_system_log(f"ROBOTRUNTIME CRASHED: {reason}", job_id)

        try: self.send_admin_alert(reason)
        except Exception: pass

        try: self.append_ui_and_system_log("CRASH! All automations halted. Admin is notified.", blank_line_before=True)
        except Exception: pass

        #do we need this, already sent?? is possible email was not sent?
        #try:
        #    if job_id is not None:
        #        self.incoming_mail_handler.send_final_job_reply(job_id, status="FAILED")
        #except Exception: pass

        try: self.recording_service.stop()
        except Exception: pass

        try: self.ui.post_hide_recording_overlay()
        except Exception: pass

        try: self.ui.post_status("safestop")
        except Exception:
            try: self.ui.post_shutdown()
            except: os._exit(1)
            time.sleep(3)
            os._exit(1)  #kill if still alive after 3 sec soft-shutdown
        
        self.enter_degraded_mode()


    def enter_degraded_mode(self) -> Never:
        self.append_system_log("running")
        
        while True:
            try:
                time.sleep(1)

                if os.path.isfile("reboot.flag"):
                    try: os.remove("reboot.flag")
                    except Exception: pass
                    self.append_system_log("reboot command received by reboot.flag")
                    self.do_reboot_sequence()

                inbox_path = self.mail_backend.fetch_next_from_inbox()
                if not inbox_path:
                    continue

                processing_path = self.mail_backend.claim_to_processing(inbox_path)
                mail = self.mail_backend.parse_processing_mail(processing_path)
                self.append_ui_and_system_log(f"email from {mail.sender_email}", blank_line_before=True)

                if not self.friends_repo.is_allowed_sender(mail.sender_email):
                    self.append_ui_and_system_log("--> rejected (not in friends.xlsx)")
                    self.mail_backend.delete_from_processing(mail)
                    continue
   
                if mail.subject.strip().lower() == "reboot1234":
                    self.append_system_log(f"reboot command received from {mail.sender_email}")
                    try: self.mail_backend.reply_and_delete(mail, subject=f"got it! re: {mail.subject}", body="Reboot command received")
                    except Exception: pass
                    self.append_ui_and_system_log("--> reboot command")
                    self.do_reboot_sequence()
                
                try: self.mail_backend.reply_and_delete(mail, subject=f"FAILED re: {mail.subject}", body="Robot is out-of-service. Your email was deleted.")
                except Exception: pass
                try:
                    job_id = self.generate_job_id()
                    now = datetime.datetime.now()
                    self.audit_repo.insert_into_db(
                        job_id=job_id,
                        email_address=mail.sender_email,
                        email_subject=mail.subject,
                        job_start_date=now.strftime("%Y-%m-%d"),
                        job_start_time=now.strftime("%H:%M:%S"),
                        job_status="REJECTED",
                        error_code="SAFESTOP",
                    )
                except Exception: pass
                self.append_ui_and_system_log("--> rejected (safestop)")
            
            except Exception as err:
                self.append_system_log(f"err: {err}")


    def do_reboot_sequence(self) -> Never:
        try:
            with open("reboot_helper.py", "w", encoding="utf-8") as f:
                f.write('''import time, sys, subprocess \ntime.sleep(2)  \nsubprocess.Popen([sys.executable, "main.py"], start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,close_fds=True,)''')
            
            time.sleep(1)
            subprocess.Popen([sys.executable, "reboot_helper.py"], start_new_session=True)
        except:
            try: self.ui.post_shutdown()
            except: os._exit(1)
        time.sleep(3)
        os._exit(1)  #kill if still alive after 3 sec soft-shutdown


    def poll_for_stop_flag(self):
        self.append_system_log("poll_for_stop_flag() alive")

        while True:
            time.sleep(1)
            
            if os.path.isfile("stop.flag"):
                try: os.remove("stop.flag")
                except Exception: pass
                
                try: self.ui.post_shutdown() #request soft-exit from g if possible
                except Exception: os._exit(1)
                
                time.sleep(3)
                os._exit(1)  #kill if still alive after 3 sec soft-exit 


    def send_admin_alert(self, reason):
         mail=MailItemCandidate(
            sender_email="adminATcompany.com",
            subject="safestop entered",
            body=f"Reson for safestop: {reason}",
            id="dummy",
            sender_name="dummy",
            headers={}
            )
         
         self.mail_backend.send_reply(mail, subject=mail.subject, body=mail.body)
                


# for core automation logic with job processing pipeline - "the heart"
class RobotRuntime:

    def __init__(self, ui):

        self.in_dev_mode = True

        self.ui = ui
        self.handover_repo = HandoverRepository(self.append_system_log)  #nu äger Runtime en handover_repo (som får med append-metod)
        self.friends_repo = FriendsRepository(self.append_system_log)
        self.audit_repo = AuditRepository(self.append_system_log)
        self.network_service = NetworkService(self.append_system_log)
        self.recording_service = RecordingService(self.append_system_log)
        self.safestop_controller = SafeStopController(self.append_system_log, self.append_ui_and_system_log, self.recording_service, ui, DevFileMailBackend(self.append_system_log, pipeline_root="own_inbox"), self.audit_repo, self.generate_job_id, self.friends_repo) 
        self.job_handlers = {"ping": JobPingHandler(), "job1": Job1Handler(), "job2": Job2Handler(), "job3": Job3Handler(),}
        self.scheduledjobs_master = ScheduledJobsMaster(append_system_log=self.append_system_log, append_ui_and_system_log=self.append_ui_and_system_log, update_ui_status=self.update_ui_status, generate_job_id=self.generate_job_id, is_within_operating_hours=self.is_within_operating_hours, network_service=self.network_service, recording_service=self.recording_service, handover_repo=self.handover_repo, audit_repo=self.audit_repo, ui_dot_post_show_recording_overlay=self.ui.post_show_recording_overlay, safestop_controller=self.safestop_controller, job_handlers=self.job_handlers, in_dev_mode=self.in_dev_mode)
        self.mail_master = MailMaster(self.in_dev_mode, self.append_system_log, self.append_ui_and_system_log, self.friends_repo, self.is_within_operating_hours, self.network_service, self.recording_service, self.generate_job_id, self.audit_repo, self.update_ui_status, self.refresh_jobs_done_today_display, self.ui.post_show_recording_overlay, self.job_handlers, self.safestop_controller)

        #self.erp_query_service = DevERPBackend()

    def initialize_runtime(self):
        VERSION = 0.4
        self.append_system_log(f"RuntimeThread started, version={VERSION}")

        self.handover_repo.write({"ipc_state":"idle"}) # no-resume policy, always cold start

        # cleanup helpers
        for fn in ["stop.flag", "reboot.flag", "reboot_helper.py"]:
            try: os.remove(fn)
            except Exception: pass

        self.network_service.has_network_access()

        atexit.register(self.recording_service.stop) #extra protection during normal python exit
        self.recording_service.stop() #stop any remaing recordings
        self.recording_service.cleanup_aborted_recordings()

        try: self.friends_repo.ensure_friends_file_exists()
        except Exception as err: self.safestop_controller.enter_safestop(reason=err)

        try: self.friends_repo.reload_if_changed(force_reload=True)
        except Exception as err: self.safestop_controller.enter_safestop(reason=err)

        try: self.audit_repo.create_db_if_needed()
        except Exception as err: self.safestop_controller.enter_safestop(reason=err)

        try: self.refresh_jobs_done_today_display()
        except Exception: pass

        result = self.audit_repo.has_unreplied_finished_jobs()
        print("has_unreplied_finished_jobs", result)
        #if result: os._exit(1)


    def run(self) -> None:
        self.initialize_runtime()
        self.prev_ui_status = None

        sleep_s = 1       
        watchdog_timeout = 600 #10 min

        prev_ipc_state = None
        watchdog_time_limit = None

        while True:
            try:

                handover_data = self.handover_repo.read()
                ipc_state = handover_data.get("ipc_state")
                
                #dispatch
                if ipc_state == "idle":
                    self.check_for_jobs()  #set ipc_state=job_queued if RPA proccessing needed
                    time.sleep(sleep_s)

                elif ipc_state == "job_queued":  #RPA-poll trigger
                    time.sleep(sleep_s)

                elif ipc_state == "job_running":  #RPA set ipc_state=job_running when job fetched
                    time.sleep(sleep_s)

                elif ipc_state == "job_verifying":       #RPA set ipc_state=job_verifying when job completed
                    self.verification_hub_all_jobs(handover_data)

                elif ipc_state == "safestop":  #only RPA can trigger 'safestop' this way 
                    self.safestop_controller.enter_safestop(reason="crash_in_conventional_rpa", job_id=handover_data.get("job_id"))
                    

                #log all ipc_state transitions
                if ipc_state != prev_ipc_state:
                    self.append_system_log(f"state transition detected by CPU-poll: {prev_ipc_state} -> {ipc_state}")
                    print("state is", ipc_state)
                    self.update_ui_status(ipc_state)

                    #note handover time or last external RRA activity
                    if ipc_state in ("job_queued", "job_running"):
                        watchdog_time_limit = time.time()  # float for seconds passed since epoch
                    else:
                        watchdog_time_limit = None
                   
                    #update DB 
                    if ipc_state == "job_running":
                        self.audit_repo.update_db(job_id=handover_data.get("job_id"), job_status="RUNNING")
                    

                #detect if external RPA takes too long time to start or finish
                if watchdog_time_limit and ipc_state in ("job_queued", "job_running") and time.time() - watchdog_time_limit > watchdog_timeout:
                    self.audit_repo.update_db(
                        job_id=handover_data.get("job_id"),
                        job_status="FAILED",
                        error_code="TIMEOUT",
                        error_explanation="No progress for 10 minutes",
                    )
                    watchdog_time_limit = None
                    self.safestop_controller.enter_safestop(reason="RPA timeout - no progress for 10 min", job_id=handover_data.get("job_id"))
                    # self.incoming_mail_handler.reply_and_delete(email_id=handover_data.get("email_id"), job_id=handover_data.get("job_id"), message="The robot could not complete your job because the automation timed out. The job has failed and an administrator has been notified. Watch the screen-recording")    
                
                prev_ipc_state = ipc_state

                
                #print(".", end="", flush=True)
                 
            except Exception:
                reason = traceback.format_exc()
                self.safestop_controller.enter_safestop(reason=reason)             


    def refresh_jobs_done_today_display(self):

        count = self.audit_repo.count_completed_jobs_today()
        self.ui.post_jobs_done_today(count)


    def update_ui_status(self, arg=None) -> None:
        try:
            handover_data = self.handover_repo.read()
            ipc_state = handover_data.get("ipc_state")
        except Exception:
            ipc_state = None
        

        if ipc_state == "safestop":
            ui_status = "safestop"

        elif arg == "working" or ipc_state in ("job_queued", "job_running", "job_verifying"):
            ui_status = "working"

        elif self.network_service.network_state is False:
            ui_status = "no network"

        elif not self.is_within_operating_hours():
            ui_status = "ooo"

        else:
            ui_status = "online"

        if self.prev_ui_status != ui_status:
            self.ui.post_status(ui_status)
            self.prev_ui_status = ui_status


    def append_ui_and_system_log(self, text:str, job_id=None, blank_line_before: bool = False) -> None:
        
        self.ui.post_log(text, blank_line_before)
        self.append_system_log(text.replace("\n", " "), job_id=job_id)


    def append_system_log(self, event_text: str, job_id=None):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
                with open("system.log", "a", encoding="utf-8") as f:
                    f.write(log_line + "\n")
                    f.flush()
                return

            except Exception as err:
                #last_err = err
                print(f"WARN: retry {i+1}/7 from append_system_log():", err)
                time.sleep(i + 1)

        #raise RuntimeError(f"append_system_log() failed after 7 attempts: {last_err}")
 

    def check_for_jobs(self) -> bool:
        
        mailflow_result = self.mail_master.process_one_cycle()
        if mailflow_result.handover_data is not None:
            self.handover_repo.write(mailflow_result.handover_data)
            return True
        
        if mailflow_result.handled_anything:
            return True   # mailflow is allowed to starve scheduledjobs 

        scheduledjobsflow_result = self.scheduledjobs_master.process_one_cycle()
        if scheduledjobsflow_result.handover_data is not None:
            self.handover_repo.write(scheduledjobsflow_result.handover_data)
            return True
   

        return False

            
    def verification_hub_all_jobs(self, handover_data):

        job_id = handover_data.get("job_id")
        job_type = handover_data.get("job_type")

        self.append_system_log(f"fetched: {handover_data}", job_id)
        self.audit_repo.update_db(job_id=job_id, job_status="VERIFYING")

        time.sleep(1) #simulate verification time

 

        handler = self.job_handlers.get(job_type)

        if handler is None:
            self.finalize_verification(handover_data, f"No handler for job_type={job_type}")
            self.handover_repo.write({"ipc_state": "idle"})
            return

        try:
            result = handler.verify_result(handover_data)
        except Exception as err:
            result = f"verification crash: {err}"

        self.finalize_verification(handover_data, result)
        self.handover_repo.write({"ipc_state": "idle"})    



        

    def finalize_verification(self, handover_data, result):
        job_id = handover_data.get("job_id")
        job_type = handover_data.get("job_type")

        error_code = None
        error_message = None
    
        if result == "ok":
            job_status = "DONE"
        else:
            job_status = "FAILED"
            error_message = result
            error_code="VALIDATION_FAILED"

        self.audit_repo.update_db(job_id=job_id, job_status=job_status, error_code=error_code, error_explanation=error_message, job_finish_time=datetime.datetime.now().strftime("%H:%M:%S")) 

        self.append_ui_and_system_log(f"--> {job_status.lower()} ({job_type})", job_id)
            
        self.recording_service.stop(job_id)
        try: self.ui.post_hide_recording_overlay()
        except Exception: pass

        if not self.in_dev_mode: self.recording_service.upload_recording(job_id=job_id)

        self.refresh_jobs_done_today_display()


    def generate_job_id(self) -> int:
        """ makes the unique value assosiated with this job """

        job_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        
        # bullet-proof dublicate-value prevention
        while self.audit_repo.get_most_recent_job() >= int(job_id):
            time.sleep(1)
            job_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        self.append_system_log("new id", int(job_id))
        return int(job_id)

    
    def is_within_operating_hours(self) -> bool:
        #return False
        now = datetime.datetime.now().time()
        result = datetime.time(5,0) <= now <= datetime.time(23,0)
        self.append_system_log(f"returing: {result}")
        return result
    

# for dashbouard with live log for operator quick overview - "the face"
class DashboardUI:
    # Tkinter dashboard for monitoring
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

                # --- Layout: root uses grid ---
        self.root.grid_rowconfigure(1, weight=1)
        self.root.grid_columnconfigure(0, weight=1)


    def _build_header(self, bg_color, text_color):
        self.header = tk.Frame(self.root, bg=bg_color)
        
        self.header.grid(row=0, column=0, sticky="ew")
        self.header.grid_columnconfigure(2, weight=1)  
        self.header.grid_rowconfigure(0, weight=1)  

               # --- Header content ---
        self.rpa_text_label = tk.Label(self.header, text="RPA:", fg=text_color, bg=bg_color, font=("Arial", 100, "bold"))  #snyggare: "Segoe UI"
        self.rpa_text_label.grid(row=0, column=0, padx=16, pady=16, sticky="w")
        self.rpa_status_label = tk.Label(self.header, text="", fg="red", bg=bg_color, font=("Arial", 100, "bold"))
        self.rpa_status_label.grid(row=0, column=1, padx=16, pady=16, sticky="w")
        self.status_dot = tk.Label(self.header, text="", fg="#22C55E", bg=bg_color, font=("Arial", 50, "bold"))
        self.status_dot.grid(row=0, column=2, sticky="w")


        # --- Jobs done today (counter + label in same) ---
        self.jobs_counter_frame = tk.Frame(self.header, bg=bg_color)
        self.jobs_counter_frame.grid(row=0, column=3, sticky="ne", padx=40, pady=30)
        self.jobs_counter_frame.grid_rowconfigure(0, weight=1)
        self.jobs_counter_frame.grid_columnconfigure(0, weight=1)


        # --- NORMAL VIEW (jobs done today) ---
        
        self.jobs_normal_view = tk.Frame(self.jobs_counter_frame, bg=bg_color)
        self.jobs_normal_view.grid(row=0, column=0, sticky="nsew")
        self.jobs_normal_view.grid_columnconfigure(0, weight=1)

        self.jobs_done_label = tk.Label(    self.jobs_normal_view,    text="0",    fg=text_color,    bg=bg_color,    font=("Segoe UI", 140, "bold"),       anchor="e",        justify="right")
        self.jobs_done_label.grid(row=0, column=0, sticky="e")

        self.jobs_counter_text = tk.Label(            self.jobs_normal_view,            text="jobs done today",            fg="#A0A0A0",            bg=bg_color,            font=("Arial", 14, "bold"),            anchor="e"        )
        self.jobs_counter_text.grid(row=1, column=0, sticky="e", pady=(0, 6))

        # --- SAFESTOP VIEW (stort X) ---
        self.jobs_error_view = tk.Frame(self.jobs_counter_frame, bg=bg_color)
        self.jobs_error_view.grid(row=0, column=0, sticky="nsew")

        self.safestop_x_label = tk.Label(            self.jobs_error_view,                        text="X",            bg="#DC2626",            fg="#FFFFFF",            font=("Segoe UI", 140, "bold")        ) #text="✖",
        self.safestop_x_label.pack(expand=True)


        # show normal view at startup
        self.jobs_normal_view.tkraise()

        #online-status animation
        self._online_animation_after_id = None
        self._online_pulse_index = 0

        #"working..."-status animation
        self._working_animation_after_id = None
        self._working_dots = 0


    def _build_body(self,bg_color, text_color):
        self.body = tk.Frame(self.root, bg=bg_color)        
        self.body.grid(row=1, column=0, sticky="nsew")
        self.body.grid_rowconfigure(0, weight=1)
        self.body.grid_columnconfigure(0, weight=1)

                        # --- Body content ---
        log_and_scroll_container = tk.Frame(self.body, bg=bg_color)
        log_and_scroll_container.grid(row=0, column=0, sticky="nsew")
        log_and_scroll_container.grid_rowconfigure(0, weight=1)
        log_and_scroll_container.grid_columnconfigure(0, weight=1)

        #the right-hand side scrollbar
        scrollbar = tk.Scrollbar(log_and_scroll_container, width=23, troughcolor="#0F172A", bg="#1E293B", activebackground="#475569", bd=0, highlightthickness=0, relief="flat")
        scrollbar.grid(row=0, column=1, sticky="ns")

        #the 'console'
        self.log_text = tk.Text(log_and_scroll_container, yscrollcommand=scrollbar.set, bg=bg_color, fg=text_color, insertbackground="black", font=("DejaVu Sans Mono", 20), wrap="none", state="disabled", bd=0,highlightthickness=0) #glow highlightbackground="#1F2937", highlightthickness=1 
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar.config(command=self.log_text.yview)


    def _build_footer(self,bg_color, text_color):
        self.footer = tk.Frame(self.root, bg=bg_color)        
        self.footer.grid(row=2, column=0, sticky="nsew")
        self.footer.grid_rowconfigure(0, weight=1)
        self.footer.grid_columnconfigure(0, weight=1)
        
                        # ---- Footer content ---
        self.last_activity_label = tk.Label(self.footer, text="last activity: xx:xx", fg="#A0A0A0", bg=bg_color, font=("Arial", 14, "bold"), anchor="e")
        self.last_activity_label.grid(row=0, column=1, padx=8, pady=16)


    def debug_grid(self,widget):
        #highlights all gris with red
        for child in widget.winfo_children():
            try:
                child.configure(highlightbackground="red", highlightthickness=1)
            except Exception:
                pass
            self.debug_grid(child)


    def update_status_display(self, status=None):
        #sets the status

        #stops any ongoing animations
        self._stop_online_animation()
        self._stop_working_animation()
        self.status_dot.config(text="")


        #changes text
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
        #hides recording window
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
        #appends the console-style log
        self.log_text.config(state="normal")
        now = datetime.datetime.now().strftime("%H:%M")

        if blank_line_before:
            self.log_text.insert("end", "\n")

        self.log_text.insert("end", f"[{now}] {log_line}\n")
        self.log_text.config(state="disabled")
        self.log_text.see("end")


    def shutdown(self) -> Never | None:
        if self._closing: return
        self._closing = True

        try: self.robot_runtime.recording_service.stop()
        except Exception: pass

        self.root.destroy()

    #wrappers
    def post_status(self, status: str) -> None:
        self.root.after(0, lambda: self.update_status_display(status))

    def post_log(self, text: str, blank_line_before: bool = False) -> None:
        self.root.after(0, lambda: self.append_ui_log(text, blank_line_before))

    def post_show_recording_overlay(self) -> None:
        self.root.after(0, self.show_recording_overlay)

    def post_hide_recording_overlay(self) -> None:
        self.root.after(0, self.hide_recording_overlay)

    def post_jobs_done_today(self, n: int) -> None:
        self.root.after(0, lambda: self.set_jobs_done_today(n))
    
    def post_shutdown(self,) -> None:
        self.root.after(0, self.shutdown)


# for testing: a simulator of an external RPA-system that does the dirty-work - "the hands"
class RPASimulator:
    """ simulating the behaviour of the external RPA software"""
    #temporary! ignore this class in all eveluations

    def __init__(self):
      
        #self.run()  #remove if started from main.
        with open("handover.txt", "w", encoding="utf-8") as f:
            f.write("ipc_state=idle")
        time.sleep(1)


    def refresh_handover_txt(self) -> dict:
        handover = {}

        with open("handover.txt", "r", encoding="utf-8") as f:
            for row in f:   
                row = row.strip()
                if not row: continue  
                if "=" not in row: raise ValueError(f"Invalid row in handover: {row}")
                key, value = row.split("=", 1)
                handover[key.strip()] = value.strip()
        return handover


    def claim_the_workflow(self, handover_data) -> dict:

        handover_data["ipc_state"] = "job_running"
        job_id = handover_data.get("job_id") 

        self.append_system_log(f"ipc_state: job_queued -> job_running", job_id)
        self.append_system_log(f"received:{handover_data}",job_id)
        
        dir_path = os.path.dirname(os.path.abspath("handover.txt"))
        fd, temp_path = tempfile.mkstemp(dir=dir_path)

        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            for key, value in handover_data.items():
                if value is None:
                    value = ""
                tmp.write(f"{key}={value}\n")

            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(temp_path, "handover.txt")

        return handover_data


    def mark_workflow_ready_for_verification_step(self, handover_data) -> dict:

        handover_data["ipc_state"] = "job_verifying"
        job_id = handover_data.get("job_id") 

        self.append_system_log(f"ipc_state: job_queued -> job_running", job_id)
        self.append_system_log(f"received:{handover_data}",job_id)
        
        dir_path = os.path.dirname(os.path.abspath("handover.txt"))
        fd, temp_path = tempfile.mkstemp(dir=dir_path)

        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            for key, value in handover_data.items():
                if value is None:
                    value = ""
                tmp.write(f"{key}={value}\n")

            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(temp_path, "handover.txt")

        return handover_data


    #läs handover.txt och kolla ifall "queued"
    def run(self):
        self.append_system_log("RPASimulator alive")

        #do a time-check to not start old jobs? 

        while(1):

            time.sleep(1)
            
            self.check_for_reboot_flag()

            handover_data = self.refresh_handover_txt()
            ipc_state = handover_data.get("ipc_state")

            if ipc_state != "job_queued":
                continue

            #RPASimulator now claiming the workflow
            handover_data = self.claim_the_workflow(handover_data)

            
            job_type = handover_data.get("job_type")
            print("job_type is", job_type)

            if job_type == "adasd":
                payload = handover_data.get("payload")
                self.DEV_simulation_of_RPA_correcting_qty_in_ERP("order_number", "material_available")

            




            processtid= random.randint(2,4)
            time.sleep(processtid)
            self.append_system_log(f"screen_1 completed")
            time.sleep(1)
            self.append_system_log(f"screen_2 completed")

            #mark the workflow completed in RPASimulator
            self.mark_workflow_ready_for_verification_step(handover_data)
            self.append_system_log(f"done, ipc_state: job_running -> job_verifying", handover_data.get("job_id"))


    def check_for_reboot_flag(self):
        import os.path
        if os.path.isfile("reboot_ready.flag"):
            os.remove("reboot_ready.flag")
            print("reboot_ready.flag found, rebooting main.py")
            time.sleep(2)
            import subprocess, sys
            #/home/elias/environments/venv/bin/python3 for venv
            subprocess.run([sys.executable, "main.py",], start_new_session=True)


    def append_system_log(self, text: str, job_id=None):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #+ str(datetime.datetime.now().microsecond)[:-4]
   
        job_part = f" | JOB {job_id}" if job_id else ""
        message = f"{timestamp} | RPA{job_part} | {text} \n"

        for i in range(5):
            try:
                with open("system.log", "a", encoding="utf-8") as f:
                    f.write(message)
                    f.flush()
                return 

            except Exception as e:
                print(f"{i}st warning from append_system_log():", e)
    

    def DEV_simulation_of_RPA_correcting_qty_in_ERP(self, order_number, material_available, path="table_in_ERP.xlsx") -> bool:

        wb = load_workbook(path)
        ws = wb.active

        assert ws is not None

        for row in ws.iter_rows(min_row=2):
            cell_order_number = row[0].value

            if str(cell_order_number) == str(order_number):
                row[1].value = material_available  # 'correcting' the value

                wb.save(path)
                return True  # success

        return False  # ordernr not found



def main() -> None:
    ui = DashboardUI()
    robot_runtime = RobotRuntime(ui)
    rpa_simulator = RPASimulator()


    ui.attach_runtime(robot_runtime)

    threading.Thread(target=robot_runtime.run, daemon=True).start()
    threading.Thread(target=robot_runtime.safestop_controller.poll_for_stop_flag, daemon=True).start()
    threading.Thread(target=rpa_simulator.run, daemon=True).start() #replace with external RPA when deployed

    ui.run()


if __name__ == "__main__":
    main()