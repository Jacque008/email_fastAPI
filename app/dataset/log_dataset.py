from __future__ import annotations
from typing import Dict, List, Any
import pandas as pd
from dataclasses import dataclass, field
from ..services.log import LogService

@dataclass
class LogDataset:
    """
    Dataset class for chronological log generation - follows DataFrame-first pattern
    """
    df: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Service initialized in __post_init__
    log_service: LogService = field(init=False)

    def __post_init__(self):
        """Initialize services after dataclass creation"""
        self.log_service = LogService()
    
    def do_chronological_log(self) -> pd.DataFrame:
        """
        Generate chronological logs for all errands in the internal DataFrame - main processing method

        Returns:
            DataFrame with log results
        """
        if self.df.empty:
            return pd.DataFrame()

        results = []
        for _, row in self.df.iterrows():
            try:
                errand_number = row.get('errand_number')
                if not errand_number:
                    continue
                
                self.log_service.setup_query_conditions(errand_number)
                base_data = self.log_service.get_errand_base_data()

                if base_data.empty:
                    error_result = {
                        'Title': f"Errand {errand_number} not found",
                        'Chronological_Log': "No data found for the specified criteria",
                        'AI_Analysis': "No analysis available - no data found",
                        'error_message': f"No errand found with number: {errand_number}",
                        'Error_Combined_Info': f"No errand found with number: {errand_number}",
                        'Summary_Combined_Info': None
                    }
                    results.append(error_result)
                    continue

                errand_id = base_data['errandId'].iloc[0]
                log_components = self._generate_all_log_components(base_data)

                group_log, group_ai = self.log_service.create_formatted_log(
                    base_data,
                    *log_components
                )
                
                if errand_id not in group_log or errand_id not in group_ai:
                    error_result = {
                        'Title': f"Errand {errand_number}",
                        'Chronological_Log': "No log entries found for this errand",
                        'AI_Analysis': "No analysis available - no log entries found",
                        'error_message': "No log entries generated for this errand",
                        'Error_Combined_Info': "No log entries generated for this errand",
                        'Summary_Combined_Info': None
                    }
                    results.append(error_result)
                    continue

                log_data = group_log[errand_id]
                ai_analysis = group_ai[errand_id]

                result = {
                    'Title': log_data["title"],
                    'Chronological_Log': log_data["content"],
                    'AI_Analysis': ai_analysis,
                    'error_message': None,
                    'Error_Combined_Info': None,
                    'Summary_Combined_Info': log_data["content"]  # Use log content as summary when no error
                }
                results.append(result)

            except Exception as e:
                error_result = {
                    'Title': f"Error processing {errand_number}",
                    'Chronological_Log': "An error occurred while generating the log",
                    'AI_Analysis': "No analysis available due to processing error",
                    'error_message': f"Processing error: {str(e)}",
                    'Error_Combined_Info': f"Processing error: {str(e)}",
                    'Summary_Combined_Info': None
                }
                results.append(error_result)

        return pd.DataFrame(results)
    
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
        create_log = self.log_service.create_errand_log(base_data)
        if not create_log.empty:
            components.append(create_log)

        # Create send to IC log
        send_log = self.log_service.send_to_ic_log(base_data)
        if not send_log.empty:
            components.append(send_log)

        # Get email data and create update log
        email_log, email_base = self.log_service.get_email_data()
        if not email_log.empty:
            components.append(email_log)

        # Create update errand log (depends on email data)
        update_log = self.log_service.create_update_errand_log(base_data, email_base)
        if not update_log.empty:
            components.append(update_log)

        # Get chat data
        chat_log = self.log_service.get_chat_data(base_data)
        if not chat_log.empty:
            components.append(chat_log)

        comment_log = self.log_service.get_comment_data(base_data)
        if not comment_log.empty:
            components.append(comment_log)

        vet_fee_log = self.log_service.get_vet_fee_data()
        if not vet_fee_log.empty:
            components.append(vet_fee_log)
            
        invoice_log = self.log_service.get_invoice_data()
        if not invoice_log.empty:
            components.append(invoice_log)

        # Get payment data
        payment_log = self.log_service.get_payment_data()
        if not payment_log.empty:
            components.append(payment_log)

        # Get cancellation data
        cancel_log = self.log_service.get_cancellation_data()
        if not cancel_log.empty:
            components.append(cancel_log)

        # Get cancellation reversal data
        remove_cancel_log = self.log_service.get_reversal_data()
        if not remove_cancel_log.empty:
            components.append(remove_cancel_log)
        
        return components

    def get_statistics(self) -> pd.DataFrame:
        """
        Get statistics for processed logs DataFrame

        Returns:
            DataFrame with statistics for each log
        """
        if self.df.empty:
            return pd.DataFrame()

        # Process logs if not already done
        result_df = self.do_chronological_log()

        if result_df.empty:
            return pd.DataFrame()

        stats_results = []
        for _, row in result_df.iterrows():
            if row.get('error_message'):
                stats = {
                    "has_error": True,
                    "error_message": row.get('error_message'),
                    "entry_count": 0,
                    "has_ai_analysis": False
                }
            else:
                # Count log entries (approximate based on bullet points)
                log_content = row.get('Chronological_Log', '')
                entry_count = log_content.count("• At") if log_content else 0

                # Check if AI analysis indicates high risk
                ai_analysis = row.get('AI_Analysis', '')
                has_high_risk = "Hög" in ai_analysis if ai_analysis else False

                # Check for payment discrepancies
                log_title = row.get('Title', '')
                has_payment_discrepancy = "Betalningsavvikelse:" in log_title and "Nej" not in log_title

                stats = {
                    "has_error": False,
                    "error_message": None,
                    "entry_count": entry_count,
                    "has_ai_analysis": bool(ai_analysis),
                    "has_high_risk": has_high_risk,
                    "has_payment_discrepancy": has_payment_discrepancy,
                    "log_length": len(log_content) if log_content else 0
                }

            stats_results.append(stats)

        return pd.DataFrame(stats_results)
