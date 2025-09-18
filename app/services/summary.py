import pandas as pd
from typing import Dict, List, Tuple, Optional
from groq import Groq, APIConnectionError, RateLimitError, APIStatusError
from .base_service import BaseService
from .utils import fetchFromDB, tz_convert, skip_thinking_part, get_groq_client, groq_chat_with_fallback

class SummaryService(BaseService):
    """Service for generating AI-powered summaries of communications"""
    
    def __init__(self):
        super().__init__()
        
        # Initialize services and processor
        from .services import DefaultServices
        self.services = DefaultServices()
        self.processor = self.services.get_processor()
        
        self.summary_chat_query = self.queries['summaryChat'].iloc[0]
        self.summary_email_query = self.queries['summaryEmail'].iloc[0] 
        self.summary_comment_query = self.queries['summaryComment'].iloc[0]
        self.model = pd.read_csv(f"{self.folder}/model.csv")['model'].iloc[0]
        self.system_prompt = {
            "role": "system",
            "content": (
                "You are a professional and efficient assistant from Sweden, "
                "specializing in accurately summarizing conversations in Swedish. "
                "Your summaries must be clear, concise, accurate, and well-structured. "
                "Format the summary as bullet points, starting each point with '*   '. "
                "Ensure precision and avoid unnecessary information. "
                "Do not translate the text.\n\n"
                "Background information: "
                "- 'Clinic' refers to veterinary clinics that provide medical care for pets. After treating a pet, clinics are responsible for submitting claims to insurance companies, providing accurate and complete information required for reimbursement. "
                "- 'Insurance company' refers to the entity responsible for processing claims submitted by clinics. They request the necessary information, make claim decisions, and ensure timely payments. "
                "- 'DRP' (direktreglering company) acts as an intermediary platform connecting clinics and insurance companies. All communication between clinics and insurance companies occurs via the DRP platform. DRP is also responsible for forwarding emails and handling payments: insurance companies pay DRP, which deducts service fees before forwarding payments to clinics."
            )}
        self._groq_client = get_groq_client()
        

    
    def build_condition(self, email_id: Optional[int] = None, 
                       errand_number: Optional[str] = None,
                       reference: Optional[str] = None) -> Dict[str, str]:
        """Build SQL conditions for different data types based on input parameters"""
        conditions = {}
        
        if email_id:
            conditions['chat'] = f'er.id = (SELECT "errandId" FROM email WHERE id = {email_id})'     
            conditions['email'] = f'er.id = (SELECT "errandId" FROM email WHERE id = {email_id})'                           
            conditions['comment'] = f'(cr."emailId" = {int(email_id)} OR cr."errandId" = (SELECT "errandId" FROM email WHERE id = {email_id}))'
            
        elif reference:
            conditions['chat'] = f"ic.reference = '{reference}'"
            conditions['email'] = f"ic.reference = '{reference}'"
            conditions['comment'] = f"ic.reference = '{reference}'"
            
        elif errand_number:
             conditions['chat'] = f"er.reference = '{errand_number}'"   
             conditions['email'] = f"er.reference = '{errand_number}'"   
             conditions['comment'] = f"er.reference = '{errand_number}'"   
            
        else:
            raise ValueError("At least one identifier must be provided")
            
        return conditions
    
    def fetch_data(self, data_type: str, conditions: str) -> pd.DataFrame:
        """Efficiently fetch data with proper error handling"""
        query_map = {
            'chat': self.summary_chat_query,
            'email': self.summary_email_query,
            'comment': self.summary_comment_query
        }
        
        if data_type not in query_map:
            return pd.DataFrame()
            
        try:
            query = query_map[data_type].format(CONDITION=conditions)
            df = fetchFromDB(query)
            return df if not df.empty else pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    
    def process_chat_data(self, chat_df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean chat data with optimized operations"""
        if chat_df.empty:
            return chat_df

        chat_df = tz_convert(chat_df, 'createdAt')
        float_cols = chat_df.select_dtypes(include=['float64']).columns
        chat_df[float_cols] = chat_df[float_cols].astype('Int64')
        chat_df['source'] = None
        
        return chat_df
    
    def format_chat_message(self, chat_df: pd.DataFrame) -> Tuple[List[Dict[str, str]], Optional[str]]:
        """Format chat data for AI processing with optimized logic"""
        if chat_df.empty:
            return [], None
            
        user_prompt_template = (
            "Summarize the following chat conversation between DRP and {PART} in Swedish. "
            "Do not translate the text. Focus only on the key points of the conversation. "
            "Replace the clinic's name with 'kliniken' and the insurance company's name with 'FB' in all instances. "
            "The summary should be concise, accurate, and formatted as bullet points.\n\n{CHAT}"
        )
        
        chat_type = chat_df['type_'].iloc[0]
        message_history = []
        participant_name = None
        
        if chat_type == 'errand':
            clinic_mask = chat_df['clinicName'].notna()
            if clinic_mask.any():
                participant_name = chat_df[clinic_mask]['clinicName'].iloc[0]
            
            from_clinic_mask = chat_df['fromClinicUserId'].notna() & chat_df['fromAdminUserId'].isna()
            from_admin_mask = chat_df['fromAdminUserId'].notna() & chat_df['fromClinicUserId'].isna()
            
            chat_df.loc[from_clinic_mask, 'source'] = 'Clinic'
            chat_df.loc[from_admin_mask, 'source'] = 'DRP'

            for _, row in chat_df.iterrows():
                if pd.notna(row['message']):
                    if pd.notna(row['fromClinicUserId']) and pd.isna(row['fromAdminUserId']):
                        message_history.append({"role": "clinic", "content": row['message']})
                    elif pd.notna(row['fromAdminUserId']) and pd.isna(row['fromClinicUserId']):
                        message_history.append({"role": "DRP", "content": row['message']})
                    
        elif chat_type == 'insurance_company_errand':
            fb_mask = chat_df['insuranceCompanyName'].notna()
            if fb_mask.any():
                participant_name = chat_df[fb_mask]['insuranceCompanyName'].iloc[0]

            from_fb_mask = chat_df['fromInsuranceCompanyId'].notna() & chat_df['fromAdminUserId'].isna()
            from_admin_mask = chat_df['fromAdminUserId'].notna() & chat_df['fromInsuranceCompanyId'].isna()
            
            chat_df.loc[from_fb_mask, 'source'] = 'Insurance_Company'  
            chat_df.loc[from_admin_mask, 'source'] = 'DRP'
            
            # Build message history
            for _, row in chat_df.iterrows():
                if pd.notna(row['message']):
                    if pd.notna(row['fromInsuranceCompanyId']) and pd.isna(row['fromAdminUserId']):
                        message_history.append({"role": "insurance company", "content": row['message']})
                    elif pd.notna(row['fromAdminUserId']) and pd.isna(row['fromInsuranceCompanyId']):
                        message_history.append({"role": "DRP", "content": row['message']})
        
        if not message_history or not participant_name:
            return [], None
            
        # Build AI message
        part_name = f"{'a clinic named ' + participant_name if chat_type == 'errand' else 'an insurance company named ' + participant_name}"
        
        messages = [
            self.system_prompt,
            {
                "role": "user", 
                "content": user_prompt_template.format(PART=part_name, CHAT=message_history)
            }
        ]
        
        return messages, participant_name
    
    def process_email_data(self, email_df: pd.DataFrame) -> pd.DataFrame:
        """Process email data with optimized operations"""
        if email_df.empty:
            return email_df

        inbox = email_df[email_df['folder'] == 'inbox'].copy()
        sent = email_df[email_df['folder'] == 'sent'].copy()
        
        if not inbox.empty:
            try:
                from ..dataset.email_dataset import EmailDataset
                inbox_dataset = EmailDataset(df=inbox, services=self.services)
                processed_dataset = inbox_dataset.process_emails()
                pro = processed_dataset.to_frame()
                pro = pro.rename({'date':'createdAt'}, axis=1)
                inbox_processed = self._process_inbox_emails(inbox, pro)
            except Exception as e:
                raise e
        else:
            inbox_processed = pd.DataFrame()
            
        if not sent.empty:
            sent_processed = self._process_sent_emails(sent)
        else:
            sent_processed = pd.DataFrame()
        
        if not inbox_processed.empty and not sent_processed.empty:
            processed_email = pd.concat([inbox_processed, sent_processed], ignore_index=True)
        elif not inbox_processed.empty:
            processed_email = inbox_processed
        elif not sent_processed.empty:
            processed_email = sent_processed
        else:
            return pd.DataFrame()

        def extract_body(email_text):
            if pd.isna(email_text) or email_text == '':
                return ''
            
            body_marker = '[BODY]'
            if body_marker in email_text:
                body_start = email_text.find(body_marker) + len(body_marker)
                body_content = email_text[body_start:].strip()
                return body_content
            else:
                return email_text.strip()
        
        processed_email['email'] = processed_email['email'].apply(extract_body)

        return processed_email
    
    def _process_inbox_emails(self, inbox: pd.DataFrame, pro: pd.DataFrame) -> pd.DataFrame:
        """Process inbox emails efficiently"""
        result = pd.merge(
            inbox[['id', 'createdAt', 'subject', 'sender', 'receiver', 'folder']], 
            pro[['id', 'source', 'sendTo', 'email']], 
            on='id', how='left'
        )
        
        return result[['id', 'createdAt', 'source', 'sender', 'sendTo', 'receiver', 'subject', 'email', 'folder']]
    
    def _process_sent_emails(self, sent: pd.DataFrame) -> pd.DataFrame:
        """Process sent emails efficiently"""
        sent = tz_convert(sent, 'createdAt')
        sent['source'], sent['sender'], sent['sendTo'], sent['receiver'] = 'DRP', 'DRP', 'Other', None
        
        sent['receiver'] = sent['to'].str.lower().str.extract(f'({self.fb_ref_str})', expand=False)
        sent.loc[sent['receiver'].notna(), 'sendTo'] = 'Insurance_Company'

        drp_mask = (sent['sendTo'] == 'Other') & (sent['receiver'].isna()) & (sent['to'].str.lower().str.contains(self.drp_str, na=False))
        sent.loc[drp_mask, ['sendTo', 'receiver']] = ['DRP', 'DRP']
        
        finance_mask = (sent['sendTo'] == 'Other') & (sent['receiver'].isna())
        sent.loc[finance_mask, 'receiver'] = sent.loc[finance_mask, 'to'].str.lower().str.extract(f'(fortus|payex)', expand=False)
        sent.loc[(sent['sendTo'] == 'Other') & (sent['receiver'].notna()), 'sendTo'] = 'Finance'

        clinic_mask = (sent['sendTo'] == 'Other') & (sent['receiver'].isna())
        if clinic_mask.any() and hasattr(self, 'clinic_list'):
            clinic_data = pd.merge(
                sent[clinic_mask],  self.clinic_list[['clinicName', 'clinicEmail']], 
                left_on='to',  right_on='clinicEmail',  how='left'
            )
            sent.loc[clinic_mask, 'receiver'] = clinic_data['clinicName']
            sent.loc[(sent['sendTo'] == 'Other') & (sent['receiver'].notna()), 'sendTo'] = 'Clinic'
        
        sent['sendTo'] = sent['sendTo'].fillna('Other')
        
        sent['email'] = sent.apply(
            lambda row: self.processor.merge_html_text(row['subject'], row['textPlain'], row['textHtml']), 
            axis=1
        ).str[1]

        return sent[['id', 'createdAt', 'source', 'sender', 'sendTo', 'receiver', 'subject', 'email', 'folder']]
    
    def format_emails(self, email_df: pd.DataFrame) -> Tuple[List[Dict[str, str]], Optional[str], Optional[str]]:
        """Format email data for AI processing"""
        if email_df.empty:
            return [], None, None
            
        user_prompt_template = (
            "Summarize the following email conversation between {SOURCE} and {SENDTO} in Swedish. "
            "Provide a concise and accurate summary, focusing only on the key points. "
            "Replace the clinic's name with 'kliniken' and the insurance company's name with 'FB' in all instances.\n\n{EMAILTEXT}"
        )

        non_empty = email_df[(email_df['email'].notna()) & (email_df['email'] != '')]
        if non_empty.empty:
            return [], None, None

        email_history = []
        for _, row in non_empty.iterrows():
            role = row['source'] if pd.notna(row['source']) and row['source'] in ["Clinic", "Insurance_Company", "DRP"] else "Other"
            email_history.append({
                "role": role,
                "content": row['email'].strip()
            })
        

        email_text = "\n".join([f"{item['role']}: {item['content']}" for item in email_history])
        
        first_row = non_empty.iloc[0]
        source = (first_row['source'] or '').strip()
        send_to = (first_row['sendTo'] or '').strip()
        sender = first_row['sender']
        receiver = first_row['receiver']
        
        messages = [
            self.system_prompt,
            {
                "role": "user",
                "content": user_prompt_template.format(SOURCE=source, SENDTO=send_to, EMAILTEXT=email_text)
            }
        ]
        
        return messages, sender, receiver
    
    def process_comment_data(self, comment_df: pd.DataFrame) -> pd.DataFrame:
        """Process comment data efficiently"""
        if comment_df.empty:
            return comment_df
            
        comment_df = tz_convert(comment_df, 'createdAt')
        comment_df['source'] = 'DRP'
        
        return comment_df
    
    def format_comments(self, comment_df: pd.DataFrame) -> List[Dict[str, str]]:
        """Format comment data for AI processing"""
        if comment_df.empty:
            return []
            
        user_prompt_template = (
            "Summarize the following comments concisely and accurately in Swedish. "
            "Provide a concise and accurate summary, focusing only on the key points. "
            "Exclude all greeting words, and ensure the output does not contain blank lines or isolated '*'.\n\n{COMMENTS}"
        )

        comment_history = [row['content'].strip() for _, row in comment_df.iterrows()]
        messages = [
            self.system_prompt,
            {
                "role": "user", 
                "content": user_prompt_template.format(COMMENTS=comment_history)
            }
        ]
        
        return messages
    
    def create_combined_data(self, chat_df: pd.DataFrame, email_df: pd.DataFrame, comment_df: pd.DataFrame) -> pd.DataFrame:
        """Create combined chronological data structure"""
        cols = ['createdAt', 'source', 'content', 'combineType']
        if not chat_df.empty:
            chat_renamed = chat_df.rename(columns={'message': 'content'})
            chat_renamed["combineType"] = "chat"
        else:
            chat_renamed = pd.DataFrame(columns=cols)
        
        if not email_df.empty:
            email_renamed = email_df.rename(columns={'email': 'content'})
            email_renamed["combineType"] = "email"
        else:
            email_renamed = pd.DataFrame(columns=cols)
        
        if not comment_df.empty:
            comment_df["combineType"] = "comment"
        else:
            comment_df = pd.DataFrame(columns=cols)
            
        noEmpty = [df for df in [chat_renamed[cols], email_renamed[cols], comment_df[cols]] if not df.empty]
        combined_data = pd.concat(noEmpty, ignore_index=True).sort_values(by='createdAt', ignore_index=True)

        
        return combined_data
    
    def format_combined_message(self, combined_data: pd.DataFrame) -> List[Dict[str, str]]:
        """Format combined data for AI processing"""
        if combined_data.empty:
            return []
            
        user_prompt_template = (
            "Summarize the following conversation between the clinic, insurance company, and DRP concisely and accurately in Swedish. "
            "Capture only the key points from the email conversations and comments. "
            "Replace 'insurance company' with 'FB' in all instances. \n\n"
            "Additional instructions: "
            "- If DRP only forwarded an email, do not mention it in the summary to save space. "
            "- A forwarded email is defined as an email where most of the content is identical to the previous email, with only some template text added at the beginning or end. "
            "- If DRP performed any actions beyond forwarding emails, include those actions in the summary. "
            "- Ensure the summary is concise, well-structured, and focuses on key points relevant to the errand.\n\n{COMBINE}"
        )
        
        # Build combined history
        combined_history = []
        for _, row in combined_data.iterrows():
            source = row.get('source', '')
            combine_type = row.get('combineType', '')
            content = row.get('content', '')
            
            role = f"{combine_type} from {source}" if source in ["Clinic", "Insurance_Company", "DRP"] else "Other"
            combined_history.append({
                "role": role,
                "content": content.strip() if content else ''
            })
        
        combine_text = "\n".join([f"{item['role']}: {item['content']}" for item in combined_history])
        messages = [
            self.system_prompt,
            {
                "role": "user",
                "content": user_prompt_template.format(COMBINE=combine_text)
            }
        ]
        
        return messages
    
    def get_ai_response(self, messages: List[Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
        """Get AI response with error handling and response cleaning"""
        # llama-3.3-70b-versatile
        # deepseek-r1-distill-llama-70b
        if not messages:
            return None, "No messages to process"
            
        try:
            client = get_groq_client()
            content, used_model = groq_chat_with_fallback(client, messages, self.model)
            
            # Update model if it changed
            if used_model != self.model:
                self.model = used_model
            
            if content:
                summary = skip_thinking_part(used_model, content)
            else:
                summary = None
            return summary, None
            
        except APIConnectionError:
            return None, "The server could not be reached. Please try again later."
        except RateLimitError:
            return None, "Rate limit exceeded. Please try again later."  
        except APIStatusError as e:
            return None, f"API Error: {e.status_code}, Response: {e.response}"
        except Exception as e:
            return None, f"Unexpected error: {str(e)}"