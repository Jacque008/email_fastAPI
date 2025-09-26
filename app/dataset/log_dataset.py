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

                # Step 1: Setup query conditions in the service
                self.log_service.setup_query_conditions(errand_number)

                # Step 2: Get base errand data
                base_data = self.log_service.get_errand_base_data()

                if base_data.empty:
                    error_result = {
                        'errand_id': 0,
                        'log_title': f"Errand {errand_number} not found",
                        'log_content': "No data found for the specified criteria",
                        'ai_analysis': "No analysis available - no data found",
                        'error_message': f"No errand found with number: {errand_number}"
                    }
                    results.append(error_result)
                    continue

                errand_id = base_data['errandId'].iloc[0]

                # Step 3: Generate all log components
                log_components = self._generate_all_log_components(base_data)

                # Step 4: Create formatted chronological log
                group_log, group_ai = self.log_service.create_formatted_log(
                    base_data,
                    *log_components
                )

                # Step 5: Extract results for the specific errand
                if errand_id not in group_log or errand_id not in group_ai:
                    error_result = {
                        'errand_id': errand_id,
                        'log_title': f"Errand {errand_number}",
                        'log_content': "No log entries found for this errand",
                        'ai_analysis': "No analysis available - no log entries found",
                        'error_message': "No log entries generated for this errand"
                    }
                    results.append(error_result)
                    continue

                log_data = group_log[errand_id]
                ai_analysis = group_ai[errand_id]

                result = {
                    'errand_id': errand_id,
                    'log_title': log_data["title"],
                    'log_content': log_data["content"],
                    'ai_analysis': ai_analysis,
                    'error_message': None
                }
                results.append(result)

            except Exception as e:
                error_result = {
                    'errand_id': 0,
                    'log_title': f"Error processing {errand_number}",
                    'log_content': "An error occurred while generating the log",
                    'ai_analysis': "No analysis available due to processing error",
                    'error_message': f"Processing error: {str(e)}"
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

        # Get comment data
        comment_log = self.log_service.get_comment_data(base_data)
        if not comment_log.empty:
            components.append(comment_log)

        # Get invoice data
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
    
    # def do_batch_logs(self, log_df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Process batch log generation directly from DataFrame

    #     Args:
    #         log_df: DataFrame with columns ['errand_number']

    #     Returns:
    #         DataFrame with log results
    #     """
    #     if log_df.empty:
    #         return pd.DataFrame()

    #     self.df = log_df.copy()
    #     return self.do_chronological_log()

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
                    "errand_id": row.get('errand_id', 0),
                    "has_error": True,
                    "error_message": row.get('error_message'),
                    "entry_count": 0,
                    "has_ai_analysis": False
                }
            else:
                # Count log entries (approximate based on bullet points)
                log_content = row.get('log_content', '')
                entry_count = log_content.count("• At") if log_content else 0

                # Check if AI analysis indicates high risk
                ai_analysis = row.get('ai_analysis', '')
                has_high_risk = "Hög" in ai_analysis if ai_analysis else False

                # Check for payment discrepancies
                log_title = row.get('log_title', '')
                has_payment_discrepancy = "Betalningsavvikelse:" in log_title and "Nej" not in log_title

                stats = {
                    "errand_id": row.get('errand_id', 0),
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
