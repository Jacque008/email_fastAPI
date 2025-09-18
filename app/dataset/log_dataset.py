from __future__ import annotations
from typing import Dict, List
import pandas as pd
from dataclasses import dataclass, field
from ..services.log import LogService
from ..schemas.log import LogIn, LogOut

@dataclass
class LogDataset:
    """
    Dataset class for chronological log generation.
    Chains all atomic functions from LogService to generate complete logs.
    """
    services: LogService = field(default_factory=LogService)
    
    def generate_chronological_log(self, log_in: LogIn) -> LogOut:
        """
        Generate a complete chronological log for an errand.
        
        Args:
            log_in: Input schema containing errand number or date range
            
        Returns:
            LogOut: Complete chronological log with AI analysis
        """
        try:
            errand_number = log_in.errand_number

            # Step 1: Setup query conditions in the service
            self.services.setup_query_conditions(errand_number)
            
            # Step 2: Get base errand data
            base_data = self.services.get_errand_base_data()
            
            if base_data.empty:
                error_msg = f"No errand found with number: {errand_number}"
                log_title = f"Errand {errand_number} not found"
                    
                return LogOut(
                    errand_id=0,
                    log_title=log_title,
                    log_content="No data found for the specified criteria",
                    ai_analysis="No analysis available - no data found",
                    error_message=error_msg
                )
            
            errand_id = base_data['errandId'].iloc[0]
            
            # Step 3: Generate all log components in parallel (conceptually)
            log_components = self._generate_all_log_components(base_data)
            
            # Step 4: Create formatted chronological log
            group_log, group_ai = self.services.create_formatted_log(
                base_data, 
                *log_components
            )
            
            # Step 5: Extract results for the specific errand
            if errand_id not in group_log or errand_id not in group_ai:
                return LogOut(
                    errand_id=errand_id,
                    log_title=f"Errand {errand_number}",
                    log_content="No log entries found for this errand",
                    ai_analysis="No analysis available - no log entries found",
                    error_message="No log entries generated for this errand"
                )
            
            log_data = group_log[errand_id]
            ai_analysis = group_ai[errand_id]
            
            return LogOut(
                errand_id=errand_id,
                log_title=log_data["title"],
                log_content=log_data["content"],
                ai_analysis=ai_analysis,
                error_message=None
            )
            
        except Exception as e:
            return LogOut(
                errand_id=0,
                log_title=f"Error processing {errand_number}",
                log_content="An error occurred while generating the log",
                ai_analysis="No analysis available due to processing error",
                error_message=f"Processing error: {str(e)}"
            )
    
    def _generate_all_log_components(self, base_data: pd.DataFrame) -> List[pd.DataFrame]:
        """
        Generate all log components efficiently.
        
        Args:
            base_data: Base errand data
            
        Returns:
            List of DataFrames containing all log components
        """
        components = []
        
        # Create errand creation log
        create_log = self.services.create_errand_log(base_data)
        if not create_log.empty:
            components.append(create_log)
        
        # Create send to IC log
        send_log = self.services.send_to_ic_log(base_data)
        if not send_log.empty:
            components.append(send_log)
        
        # Get email data and create update log
        email_log, email_base = self.services.get_email_data()
        if not email_log.empty:
            components.append(email_log)
        
        # Create update errand log (depends on email data)
        update_log = self.services.create_update_errand_log(base_data, email_base)
        if not update_log.empty:
            components.append(update_log)
        
        # Get chat data
        chat_log = self.services.get_chat_data(base_data)
        if not chat_log.empty:
            components.append(chat_log)
        
        # Get comment data
        comment_log = self.services.get_comment_data(base_data)
        if not comment_log.empty:
            components.append(comment_log)
        
        # Get invoice data
        invoice_log = self.services.get_invoice_data()
        if not invoice_log.empty:
            components.append(invoice_log)
        
        # Get payment data
        payment_log = self.services.get_payment_data()
        if not payment_log.empty:
            components.append(payment_log)
        
        # Get cancellation data
        cancel_log = self.services.get_cancellation_data()
        if not cancel_log.empty:
            components.append(cancel_log)
        
        # Get cancellation reversal data
        remove_cancel_log = self.services.get_reversal_data()
        if not remove_cancel_log.empty:
            components.append(remove_cancel_log)
        
        return components
    
    def generate_multiple_logs(self, log_ins: List[LogIn]) -> List[LogOut]:
        """
        Generate chronological logs for multiple errands efficiently.

        Args:
            log_ins: List of LogIn objects to process

        Returns:
            List of LogOut objects
        """
        results = []
        
        for log_in in log_ins:
            result = self.generate_chronological_log(log_in)
            results.append(result)
        
        return results
    
    def get_log_summary_stats(self, log_output: LogOut) -> Dict:
        """
        Extract summary statistics from a log output.
        
        Args:
            log_output: Generated log output
            
        Returns:
            Dictionary containing summary statistics
        """
        if log_output.error_message:
            return {
                "errand_id": log_output.errand_id,
                "has_error": True,
                "error_message": log_output.error_message,
                "entry_count": 0,
                "has_ai_analysis": False
            }
        
        # Count log entries (approximate based on bullet points)
        entry_count = log_output.log_content.count("• At") if log_output.log_content else 0
        
        # Check if AI analysis indicates high risk
        has_high_risk = "Hög" in log_output.ai_analysis if log_output.ai_analysis else False
        
        # Check for payment discrepancies
        has_payment_discrepancy = "Betalningsavvikelse:" in log_output.log_title and "Nej" not in log_output.log_title
        
        return {
            "errand_id": log_output.errand_id,
            "has_error": False,
            "error_message": None,
            "entry_count": entry_count,
            "has_ai_analysis": bool(log_output.ai_analysis),
            "has_high_risk": has_high_risk,
            "has_payment_discrepancy": has_payment_discrepancy,
            "log_length": len(log_output.log_content) if log_output.log_content else 0
        }