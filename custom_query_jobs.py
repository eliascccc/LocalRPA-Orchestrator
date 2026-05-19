# place in main.py directory

from main import ActiveJob, PrecheckResult, VerificationResult
   
class OrderAdjustHandler:
    '''Example of a query-driven automation for demo purpose'''

    job_name = "order_adjust"

    def __init__(self, logger, erp_backend) -> None:
        self.logger = logger
        self.erp_backend = erp_backend

    # --- must-have methods --- #

    def find_next_active_jobs(self) -> list[ActiveJob]:
        active_jobs: list[ActiveJob] = [] 

        rows = self.erp_backend.order_adjust_selection_rows()

        for row_raw in rows:
            active_job = self.build_active_job_from_row(row_raw)
            active_job.job_name = self.job_name
            
            active_jobs.append(active_job)

        return active_jobs

    def precheck_and_build_payload(self, active_job: ActiveJob) -> PrecheckResult:
        """Validate the request and build the payload for the RPA tool."""

        source_ref = active_job.source_ref

        if active_job.source_data is not None:
            order_qty = active_job.source_data.get("order_qty")
            material_available = active_job.source_data.get("material_available")

            if order_qty == material_available:
                return PrecheckResult(is_success=False, public_error_message="no mismatch left to fix")

        rpatool_payload = {
            "source_ref": str(source_ref),
            "target_order_qty": material_available,
        }

        return PrecheckResult(
            is_success=True,
            rpatool_payload=rpatool_payload,
            request_summary=active_job.request_summary,
            )
    
    def verify_result(self, active_job: ActiveJob) -> VerificationResult:
        '''
        verify_result() must return:
        - success, or
        - failure with error_code=POST_HANDOVER_... and public_error_message.
        
        Other outcomes are treated as programming/system faults by RobotRuntime. 
        '''
    
        job_id = active_job.job_id

        rpatool_payload = active_job.rpatool_payload
        if not rpatool_payload:
            raise ValueError(f"missing rpatool_payload for active_job {active_job}")
        
        # get the order number/id and the target qty
        source_ref = rpatool_payload.get("source_ref")
        target_order_qty = rpatool_payload.get("target_order_qty")

        # get actual qty from ERP
        order_qty_erp = self.erp_backend.get_order_qty(source_ref)
        if order_qty_erp is None:
            return VerificationResult(
                is_success=False,
                error_code="POST_HANDOVER_VERIFICATION_TIMEOUT",
                public_error_message=f"Could not read order {source_ref} from ERP during verification.",
                )

        # compare them
        if order_qty_erp != target_order_qty:
            error_message= f"{source_ref} should be {target_order_qty}, is {order_qty_erp}"
            self.logger.system(error_message, job_id)
            return VerificationResult(
                is_success=False,
                error_code="POST_HANDOVER_VERIFICATION_MISMATCH",
                public_error_message=error_message
                )

        self.logger.system(f"OK. Should be: {target_order_qty}, is: {order_qty_erp}", job_id)
        return VerificationResult(
            is_success=True
            )

    # --- additional methods --- #

    def build_active_job_from_row(self, row) -> ActiveJob:
            '''used by a job in custom_queryjobs.py'''
                
            source_ref = row.get("source_ref")
            order_qty = row.get("order_qty")
            material_available = row.get("material_available")


            try: order_qty = int(order_qty)
            except Exception: raise ValueError(f"invalid order_qty: {order_qty}")
            try: material_available = int(material_available)
            except Exception: raise ValueError(f"invalid material_available: {material_available}")


            source_data = {
                "order_qty": order_qty,
                "material_available": material_available,
            }

            return ActiveJob(
                source_ref=str(source_ref),
                source_type="erp_query",
                source_data=source_data,
                job_name=self.job_name,
                request_summary=(f"Adjust order {source_ref} from {order_qty} to {material_available}")
            )


def build_custom_query_handlers(logger, erp_backend) -> dict:
    return {
        "order_adjust": OrderAdjustHandler(logger, erp_backend),
    }