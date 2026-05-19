# place in main.py directory

import re
from main import ActiveJob, PrecheckResult, VerificationResult

class QtyChangeHandler:
    '''Example of a personal-inbox email automation for demo purpose.'''
   
    job_name = "qty_adjust"
    
    def __init__(self, logger) -> None:
        self.logger = logger

    def can_handle(self, active_job: ActiveJob) -> bool:
        subject = str(active_job.email_subject).strip().lower()
        return self.job_name in subject

    def precheck_and_build_payload(self, active_job: ActiveJob) -> PrecheckResult:
        """Validate the request and build the payload for the RPA tool."""

        email_body = active_job.email_body
        assert email_body is not None # to satisfy pylance

        # get relevant info for qty_adjust, eg:
        order_number_match = re.search(r"order_number:\s*(.+)", email_body)
        order_number = order_number_match.group(1) if order_number_match else None

        order_qty_match = re.search(r"order_qty:\s*(.+)", email_body)
        order_qty = order_qty_match.group(1) if order_qty_match else None

        target_qty_match = re.search(r"material_available:\s*(.+)", email_body)
        target_qty = target_qty_match.group(1) if target_qty_match else None

        error_message = ""
        if order_number is None:
            error_message += "missing order_number. "
        if order_qty is None:
            error_message += "missing order_qty. "
        if target_qty is None:
            error_message += "missing target_qty. "

        if error_message:
            return PrecheckResult(is_success=False, public_error_message=error_message.strip())

        # and for any attachments, eg:
        source_data = active_job.source_data or {}
        attachment = source_data.get("attachment")

        #for attachment in attachments:
        #    print(attachment.get("filename"))

        rpatool_payload = {
            "order_number": order_number,
            "order_qty": order_qty,
            "target_order_qty": target_qty,
            "attachment": attachment,
        }

        return PrecheckResult(
            is_success=True, 
            rpatool_payload=rpatool_payload,
            request_summary=f"Change order {order_number} to {target_qty} pcs",
            )
    
    def verify_result(self, active_job: ActiveJob) -> VerificationResult:
        '''
        verify_result() must return:
        - success, or
        - failure with error_code=POST_HANDOVER_... and public_error_message.
        
        Other outcomes are treated as programming/system faults by RobotRuntime. 
        '''

        # Demo placeholder.
        # A real implementation would check the final order quantity in ERP.
        if False:
            return VerificationResult(
                is_success=False, 
                error_code="POST_HANDOVER_VERIFICATION_MISMATCH", 
                public_error_message="Expected target quantity in ERP was ___, found was ___.",)

        return VerificationResult(is_success=True)

def build_custom_personal_mail_handlers(logger) -> dict:
    return {
        "qty_adjust": QtyChangeHandler(logger),
    }
