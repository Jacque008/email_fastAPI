from __future__ import annotations
import pandas as pd
from typing import Any
from dataclasses import dataclass, field
from ..services.services import DefaultServices
from ..services.summary import SummaryService


@dataclass
class SummaryDataset:
    """Dataset class for summary generation - follows DataFrame-first pattern with pure DataFrame processing"""
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    services: DefaultServices = field(default_factory=DefaultServices)

    # Service initialized in __post_init__
    summary_service: SummaryService = field(init=False)

    def __post_init__(self):
        """Initialize services after dataclass creation"""
        self.summary_service = self.services.get_summary_service()

    def do_summary(self, use_case: str = 'api') -> pd.DataFrame:
        """
        Generate comprehensive summary based on internal DataFrame - main processing method

        Args:
            use_case: 'api' for combined summary only, 'webService' for detailed summaries

        Returns:
            DataFrame with summary results
        """
        if self.df.empty:
            return pd.DataFrame()

        try:
            results = []
            for _, row in self.df.iterrows():
                conditions = self.summary_service.build_condition(
                    email_id=row.get('emailId'),
                    errand_number=row.get('errandNumber'),
                    reference=row.get('reference')
                )

                chat_df = self.summary_service.fetch_data('chat', conditions['chat'])
                email_df = self.summary_service.fetch_data('email', conditions['email'])
                comment_df = self.summary_service.fetch_data('comment', conditions['comment'])

                # Generate summary data directly as dict instead of Pydantic model
                if use_case == 'webService':
                    result_data = self._generate_detailed_summaries_data(chat_df, email_df, comment_df)
                else:
                    result_data = self._generate_combined_summary_data(chat_df, email_df, comment_df)

                results.append(result_data)

            return pd.DataFrame(results)

        except Exception as e:
            error_result = {'error_message': f"Summary generation failed: {str(e)}"}
            return pd.DataFrame([error_result])

    def _generate_detailed_summaries_data(self, chat_df: pd.DataFrame,
                                        email_df: pd.DataFrame,
                                        comment_df: pd.DataFrame) -> dict:
        """Generate detailed summaries for webService use case - returns dict"""
        result_data = {}

        # Process chat summaries
        if not chat_df.empty:
            result_data.update(self._process_chat_summaries_data(chat_df))
        else:
            result_data['error_chat'] = 'Ingen Chatt Tillgänglig.'

        # Process email summary
        if not email_df.empty and email_df['id'].notna().any():
            result_data.update(self._process_email_summary_data(email_df))
        else:
            result_data['error_email'] = 'Ingen Email Tillgänglig.'

        # Process comment summaries
        if not comment_df.empty:
            result_data.update(self._process_comment_summaries_data(comment_df))
        else:
            result_data['error_comment_dr'] = 'Inga Kommentarer Tillgängliga för DR.'
            result_data['error_comment_email'] = 'Inga Kommentarer Tillgängliga för Email.'

        # Generate combined summary as well
        combined_result_data = self._generate_combined_summary_data(chat_df, email_df, comment_df)
        result_data['summary_combined'] = combined_result_data.get('summary_combined')
        result_data['error_combined'] = combined_result_data.get('error_combined')

        return result_data

    def _process_chat_summaries_data(self, chat_df: pd.DataFrame) -> dict:
        """Process chat data and generate summaries - returns dict"""
        result_data = {}
        try:
            # Process chat data
            processed_chat = self.summary_service.process_chat_data(chat_df)

            # Split by chat type
            chat_clinic = processed_chat[
                (processed_chat['type_'] == 'errand') &
                (processed_chat['message'].notna())
            ]
            chat_ic = processed_chat[
                (processed_chat['type_'] == 'insurance_company_errand') &
                (processed_chat['message'].notna())
            ]

            # Process clinic chat
            if not chat_clinic.empty and chat_clinic['reference'].notna().any():
                messages, clinic_name = self.summary_service.format_chat_message(chat_clinic)
                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result_data['summary_chat_with_clinic'] = summary
                    result_data['clinic_name'] = clinic_name
                    if error:
                        result_data['error_chat'] = error

            # Process insurance company chat
            if not chat_ic.empty and chat_ic['reference'].notna().any():
                messages, ic_name = self.summary_service.format_chat_message(chat_ic)
                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result_data['summary_chat_with_ic'] = summary
                    result_data['ic_name'] = ic_name
                    if error:
                        result_data['error_chat'] = error

        except Exception as e:
            result_data['error_chat'] = f"Chat processing failed: {str(e)}"

        return result_data

    def _process_email_summary_data(self, email_df: pd.DataFrame) -> dict:
        """Process email data and generate summary - returns dict"""
        result_data = {}
        try:
            # Process email data
            processed_email = self.summary_service.process_email_data(email_df)

            # Format for AI
            messages, sender, receiver = self.summary_service.format_emails(processed_email)

            if messages:
                summary, error = self.summary_service.get_ai_response(messages)
                result_data['summary_email_conversation'] = summary
                result_data['email_sender'] = sender
                result_data['email_receiver'] = receiver
                if error:
                    result_data['error_email'] = error

        except Exception as e:
            result_data['error_email'] = f"Email processing failed: {str(e)}"

        return result_data

    def _process_comment_summaries_data(self, comment_df: pd.DataFrame) -> dict:
        """Process comment data and generate summaries - returns dict"""
        result_data = {}
        try:
            # Process comment data
            processed_comment = self.summary_service.process_comment_data(comment_df)

            # Split by comment type
            comment_dr = processed_comment[processed_comment['type'] == 'Errand']
            comment_email = processed_comment[processed_comment['type'] == 'Email']

            # Process DR comments
            if not comment_dr.empty:
                messages = self.summary_service.format_comments(comment_dr)
                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result_data['summary_comment_dr'] = summary
                    if error:
                        result_data['error_comment_dr'] = error
            else:
                result_data['error_comment_dr'] = 'Inga Kommentarer Tillgängliga för DR.'

            # Process Email comments
            if not comment_email.empty:
                messages = self.summary_service.format_comments(comment_email)
                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result_data['summary_comment_email'] = summary
                    if error:
                        result_data['error_comment_email'] = error
            else:
                result_data['error_comment_email'] = 'Inga Kommentarer Tillgängliga för Email.'

        except Exception as e:
            result_data['error_comment_dr'] = f"Comment processing failed: {str(e)}"
            result_data['error_comment_email'] = f"Comment processing failed: {str(e)}"

        return result_data

    def _generate_combined_summary_data(self, chat_df: pd.DataFrame,
                                      email_df: pd.DataFrame,
                                      comment_df: pd.DataFrame) -> dict:
        """Generate combined summary for API use case - returns dict"""
        result_data = {}

        try:
            # Process all data types
            processed_chat = self.summary_service.process_chat_data(chat_df) if not chat_df.empty else pd.DataFrame()
            processed_email = self.summary_service.process_email_data(email_df) if not email_df.empty else pd.DataFrame()
            processed_comment = self.summary_service.process_comment_data(comment_df) if not comment_df.empty else pd.DataFrame()

            # Create combined chronological data
            combined_data = self.summary_service.create_combined_data(
                processed_chat,
                processed_email,
                processed_comment
            )

            if not combined_data.empty and combined_data['content'].str.strip().str.len().sum() > 0:
                # Format for AI
                messages = self.summary_service.format_combined_message(combined_data)

                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result_data['summary_combined'] = summary
                    if error:
                        result_data['error_combined'] = error
            else:
                result_data['error_combined'] = "Inga tillgängliga data"

        except Exception as e:
            result_data['error_combined'] = f"Combined summary generation failed: {str(e)}"

        return result_data

    def get_statistics(self) -> pd.DataFrame:
        """Get statistics about available data for processed summaries

        Returns:
            DataFrame with statistics for each summary request
        """
        if self.df.empty:
            return pd.DataFrame()

        stats_results = []

        try:
            for _, row in self.df.iterrows():
                conditions = self.summary_service.build_condition(
                    email_id=row.get('emailId'),
                    errand_number=row.get('errandNumber'),
                    reference=row.get('reference')
                )

                chat_df = self.summary_service.fetch_data('chat', conditions['chat'])
                email_df = self.summary_service.fetch_data('email', conditions['email'])
                comment_df = self.summary_service.fetch_data('comment', conditions['comment'])

                stats = {
                    'emailId': row.get('emailId'),
                    'errandNumber': row.get('errandNumber'),
                    'reference': row.get('reference'),
                    'chat_messages': len(chat_df),
                    'emails': len(email_df),
                    'comments': len(comment_df),
                    'total_items': len(chat_df) + len(email_df) + len(comment_df),
                    'has_data': not (chat_df.empty and email_df.empty and comment_df.empty)
                }
                stats_results.append(stats)

        except Exception as e:
            error_stats = {
                'error': f"Statistics generation failed: {str(e)}",
                'has_data': False
            }
            stats_results.append(error_stats)

        return pd.DataFrame(stats_results)

    # Removed all legacy methods - API layer handles Pydantic conversions
    # convert_to_api_format is now handled by the API layer directly