# place in main.py directory

import re
from main import ActiveJob, PrecheckResult, VerificationResult

class PoAdjustHandler:
    '''Example of a shared-inbox email automation for demo purpose.'''

    job_name = "po_adjust"

    def __init__(self, logger) -> None:
        self.logger = logger

    def can_handle(self, active_job: ActiveJob) -> bool:
        # Placeholder for mailbox-specific in scope rules, eg:
        sender = (active_job.email_address or "").strip().lower()
        subject = (active_job.email_subject or "").strip().lower()

        return (sender == "supplier1@example.com" and "order confirmation" in subject)

    def precheck_and_build_payload(self, active_job: ActiveJob) -> PrecheckResult:
        """Validate the request and build the payload for the RPA tool."""

        email_body = active_job.email_body
        assert email_body is not None # to satisfy pylance

        # get relevant info for po_adjust, eg.:
        order_number_match = re.search(r"order_number:\s*(.+)", email_body)
        order_number = order_number_match.group(1) if order_number_match else None

        confirmed_qty_match = re.search(r"confirmed_qty:\s*(.+)", email_body)
        confirmed_qty = confirmed_qty_match.group(1) if confirmed_qty_match else None

        confirmed_qty = str(confirmed_qty)

        error_message = ""
        if not confirmed_qty.isnumeric() or int(confirmed_qty) < 0:
            error_message = f"invalid confirmed_qty={confirmed_qty}. "

        if error_message:
            return PrecheckResult(is_success=False, public_error_message=error_message.strip())

        rpatool_payload = {
            "order_number": order_number,
            "confirmed_qty": confirmed_qty,
        }

        return PrecheckResult(
            is_success=True, 
            rpatool_payload=rpatool_payload,
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


def build_custom_shared_mail_handlers(logger) -> dict:
    return {
        "po_adjust": PoAdjustHandler(logger),
    }
