from __future__ import annotations
import pandas as pd
from dataclasses import dataclass, field
from ..services.services import DefaultServices
from ..schemas.summary import SummaryIn, SummaryOutWeb, SummaryOutAPI

@dataclass
class SummaryDataset:
    """Dataset class to orchestrate summary generation workflow"""
    df: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def services(self):
        if not hasattr(self, '_services'):
            self._services = DefaultServices()
        return self._services

    @property
    def summary_service(self):
        if not hasattr(self, '_summary_service'):
            self._summary_service = self.services.get_summary_service()
        return self._summary_service        

    def generate_summary(self, request: SummaryIn, use_case: str = 'api') -> SummaryOutWeb:
        """
        Generate comprehensive summary based on input parameters
        
        Args:
            request: SummaryIn object with emailId, errandNumber, or reference
            use_case: 'api' for combined summary only, 'webService' for detailed summaries
            
        Returns:
            SummaryOutWeb object with appropriate summary fields populated
        """
        try:
            conditions = self.summary_service.build_condition(
                email_id=request.emailId,
                errand_number=request.errandNumber, 
                reference=request.reference
            )
            
            chat_df = self.summary_service.fetch_data('chat', conditions['chat'])
            email_df = self.summary_service.fetch_data('email', conditions['email'])
            comment_df = self.summary_service.fetch_data('comment', conditions['comment'])
            
            result = SummaryOutWeb()
            if use_case == 'webService':
                result = self._generate_detailed_summaries(chat_df, email_df, comment_df)
            else:
                result = self._generate_combined_summary(chat_df, email_df, comment_df)
                
            return result
            
        except Exception as e:
            return SummaryOutWeb(error_message=f"Summary generation failed: {str(e)}")
    
    def _generate_detailed_summaries(self, chat_df: pd.DataFrame, 
                                   email_df: pd.DataFrame, 
                                   comment_df: pd.DataFrame) -> SummaryOutWeb:
        """Generate detailed summaries for webService use case"""
        result = SummaryOutWeb()
        
        # Process chat summaries
        if not chat_df.empty:
            result = self._process_chat_summaries(chat_df, result)
        else:
            result.error_chat = 'Ingen Chatt Tillgänglig.'
        
        # Process email summary
        if not email_df.empty and email_df['id'].notna().any():
            result = self._process_email_summary(email_df, result)
        else:
            result.error_email = 'Ingen Email Tillgänglig.'
        
        # Process comment summaries  
        if not comment_df.empty:
            result = self._process_comment_summaries(comment_df, result)
        else:
            result.error_comment_dr = 'Inga Kommentarer Tillgängliga för DR.'
            result.error_comment_email = 'Inga Kommentarer Tillgängliga för Email.'
        
        # Generate combined summary as well
        combined_result = self._generate_combined_summary(chat_df, email_df, comment_df)
        result.summary_combined = combined_result.summary_combined
        result.error_combined = combined_result.error_combined
        
        return result
    
    def _process_chat_summaries(self, chat_df: pd.DataFrame, result: SummaryOutWeb) -> SummaryOutWeb:
        """Process chat data and generate summaries"""
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
                    result.summary_chat_with_clinic = summary
                    result.clinic_name = clinic_name
                    if error:
                        result.error_chat = error
            
            # Process insurance company chat
            if not chat_ic.empty and chat_ic['reference'].notna().any():
                messages, ic_name = self.summary_service.format_chat_message(chat_ic)
                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result.summary_chat_with_ic = summary
                    result.ic_name = ic_name
                    if error:
                        result.error_chat = error
                        
        except Exception as e:
            result.error_chat = f"Chat processing failed: {str(e)}"
            
        return result
    
    def _process_email_summary(self, email_df: pd.DataFrame, result: SummaryOutWeb) -> SummaryOutWeb:
        """Process email data and generate summary"""
        try:
            # Process email data
            processed_email = self.summary_service.process_email_data(email_df)
            
            # Format for AI
            messages, sender, receiver = self.summary_service.format_emails(processed_email)
            
            if messages:
                summary, error = self.summary_service.get_ai_response(messages)
                result.summary_email_conversation = summary
                result.email_sender = sender
                result.email_receiver = receiver
                if error:
                    result.error_email = error
                    
        except Exception as e:
            raise e

            
        return result
    
    def _process_comment_summaries(self, comment_df: pd.DataFrame, result: SummaryOutWeb) -> SummaryOutWeb:
        """Process comment data and generate summaries"""
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
                    result.summary_comment_dr = summary
                    if error:
                        result.error_comment_dr = error
            else:
                result.error_comment_dr = 'Inga Kommentarer Tillgängliga för DR.'
            
            # Process Email comments
            if not comment_email.empty:
                messages = self.summary_service.format_comments(comment_email)
                if messages:
                    summary, error = self.summary_service.get_ai_response(messages)
                    result.summary_comment_email = summary
                    if error:
                        result.error_comment_email = error
            else:
                result.error_comment_email = 'Inga Kommentarer Tillgängliga för Email.'
                
        except Exception as e:
            result.error_comment_dr = f"Comment processing failed: {str(e)}"
            result.error_comment_email = f"Comment processing failed: {str(e)}"
            
        return result
    
    def _generate_combined_summary(self, chat_df: pd.DataFrame, 
                                 email_df: pd.DataFrame, 
                                 comment_df: pd.DataFrame) -> SummaryOutWeb:
        """Generate combined summary for API use case"""
        result = SummaryOutWeb()
        
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
                    result.summary_combined = summary
                    if error:
                        result.error_combined = error
            else:
                result.error_combined = "Inga tillgängliga data"
                
        except Exception as e:
            result.error_combined = f"Combined summary generation failed: {str(e)}"
            
        return result
    
    def get_summary_statistics(self, request: SummaryIn) -> dict:
        """Get statistics about available data for the given request"""
        try:
            conditions = self.summary_service.build_condition(
                email_id=request.emailId,
                errand_number=request.errandNumber,
                reference=request.reference
            )
            
            # Fetch data counts
            chat_df = self.summary_service.fetch_data('chat', conditions['chat'])
            email_df = self.summary_service.fetch_data('email', conditions['email'])
            comment_df = self.summary_service.fetch_data('comment', conditions['comment'])
            
            return {
                'chat_messages': len(chat_df),
                'emails': len(email_df),
                'comments': len(comment_df),
                'total_items': len(chat_df) + len(email_df) + len(comment_df),
                'has_data': not (chat_df.empty and email_df.empty and comment_df.empty)
            }
            
        except Exception as e:
            return {
                'error': f"Statistics generation failed: {str(e)}",
                'has_data': False
            }

    def convert_to_api_format(self, summary_result: SummaryOutWeb) -> SummaryOutAPI:
        """Convert full summary result to API format with only combined info"""
        return SummaryOutAPI(
            Summary_Combined_Info=summary_result.summary_combined,
            Error_Combined_Info=summary_result.error_combined
        )