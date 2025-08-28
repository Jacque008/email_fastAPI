"""
Errand Service - Handle errand timeline logs and risk assessment
Based on the original chronologicalLog.py
"""
import re
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from pydantic import BaseModel
from groq import Groq

from ..app.services.base_service import BaseService
from ..app.services.utils import fetchFromDB


class ErrandLogRequest(BaseModel):
    errandNumber: str


class ErrandLogResponse(BaseModel):
    errand_id: int
    log_content: str
    ai_analysis: str


class ErrandService(BaseService):
    """Errand service class for handling errand timeline logs and AI analysis"""
    
    def __init__(self, groq_client: Optional[Groq] = None):
        super().__init__()
        self.groq_client = groq_client
        self.drp_fee = 149
        self._setup_queries()
        self._setup_mappings()
        self._setup_rules()
    
    def _setup_queries(self):
        """Setup query statements"""
        try:
            self.log_base_query = self.queries['logBase'].iloc[0]
            self.log_email_query = self.queries['logEmail'].iloc[0]
            self.log_chat_query = self.queries['logChat'].iloc[0]
            self.log_comment_query = self.queries['logComment'].iloc[0]
            self.log_invoice_sp_query = self.queries['logInvoiceSP'].iloc[0]
            self.log_invoice_fo_query = self.queries['logInvoiceFO'].iloc[0]
            self.log_invoice_ka_query = self.queries['logInvoiceKA'].iloc[0]
            self.log_receive_query = self.queries['logReceive'].iloc[0]
            self.log_cancel_query = self.queries['logCancel'].iloc[0]
            self.log_remove_cancel_query = self.queries['logRemoveCancel'].iloc[0]
        except Exception as e:
            # Set default empty queries
            for attr in ['log_base_query', 'log_email_query', 'log_chat_query', 'log_comment_query',
                        'log_invoice_sp_query', 'log_invoice_fo_query', 'log_invoice_ka_query',
                        'log_receive_query', 'log_cancel_query', 'log_remove_cancel_query']:
                setattr(self, attr, "")
    
    def _setup_mappings(self):
        """Setup mapping relationships"""
        self.columns = ['errandId', 'node', 'timestamp', 'itemId', 'msg', 'involved', 'source']
        
        self.category_mapping = {
            'Auto_Reply': 'Auto-Svar',
            'Finance_Report': 'Ekonomirapport',
            'Information': 'Generell Information',
            'Settlement_Request': 'Förhandsbesked',
            'Message': 'Specifik Information',
            'Question': 'Fråga',
            'Settlement_Approved': 'Ersättningsbesked',
            'Settlement_Denied': 'Ersättning Nekad',
            'Wisentic_Error': 'Fel i Djurskador',
            'Complement_Damage_Request_Insurance_Company': 'Komplettering Skadeanmälan',
            'Other': 'Övrigt',
            'Spam': 'Spam',
            'Complement_Damage_Request_Clinic': 'Komplettering Skadeanmälan',
            'Complement_DR_Insurance_Company': 'Komplettering Direktreglering',
            'Complement_DR_Clinic': 'Komplettering Direktreglering',
            'Insurance_Validation_Error': 'Felaktig Försäkringsinfo',
            'Manual_Handling_Required': 'Manuell Hantering'
        }
    
    def _setup_rules(self):
        """Setup business rules"""
        self.basic_steps = [
            "Errand Created (Required and Errand Started)",
            "Errand Submitted to Insurance Company (Required)",
            "Email Correspondence Between Insurance Company and Clinic (Optional)",
            "Compensation Amount Updated (Required)",
            "Chat Communication Between Insurance Company, Clinic, and DRP (Optional)",
            "Comment Added (Optional)",
            "Invoice Generated (Required)",
            "Payment Received from Insurance Company (Required)",
            "Payment Received from Clinic (Required)",
            "Payment to Clinic (Required and Errand Closed)"
        ]
    
    def generate_errand_log(self, request: ErrandLogRequest) -> Dict[str, Any]:
        """Generate errand timeline log"""
        # Build query conditions - simplified implementation
        cond1 = True
        cond2 = f"e.errandNumber = '{request.errandNumber}'"
        
        # Get base data
        base = self._get_errand_base(cond1, cond2)
        if base.empty:
            return {"error": f"Data not found for errand number {request.errandNumber}"}
        
        # Get various types of data
        create = self._create_errand_entry(base)
        send = self._create_send_to_ic_entry(base)
        email, email_base = self._get_email_data(cond1, cond2)
        chat = self._get_chat_data(base, cond1, cond2)
        comment = self._get_comment_data(base, cond1, cond2)
        update = self._create_update_entry(base, email_base)
        invoice = self._create_invoice_entries(cond1, cond2)
        payment = self._create_payment_entries(cond1, cond2)
        cancel = self._create_cancel_entries(cond1, cond2)
        remove = self._create_remove_cancel_entries(cond1, cond2)
        
        # Create timeline log
        group_log, group_ai = self._create_chronological_log(
            base, create, send, email, chat, comment, 
            update, invoice, payment, cancel, remove
        )
        
        return {
            "group_log": group_log,
            "group_ai": group_ai
        }
    
    def _get_errand_base(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Get errand basic information"""
        try:
            base = fetchFromDB(self.log_base_query.format(COND1=cond1, COND2=cond2))
            float_cols = base.select_dtypes(include=['float64']).columns
            base[float_cols] = base[float_cols].astype('Int64')
            return base
        except:
            return pd.DataFrame()
    
    def _create_errand_entry(self, base: pd.DataFrame) -> pd.DataFrame:
        """Create errand creation record"""
        create = base[['errandId','errandCreaTime','errandNumber','clinicName']]
        if create.empty:
            return pd.DataFrame(columns=self.columns)
        
        create.loc[:, ['node','msg','source']] = ['Errand_Created', '', '']
        create = create.rename(columns={
            "errandCreaTime": "timestamp",
            "errandNumber": "itemId",
            "clinicName": "involved"
        }).drop_duplicates()
        
        create['timestamp'] = pd.to_datetime(create['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
        create['itemId'] = create['itemId'].apply(lambda row: 'errandNr: ' + row)
        
        return create[self.columns]
    
    def _create_send_to_ic_entry(self, base: pd.DataFrame) -> pd.DataFrame:
        """Create send to insurance company record"""
        send = base.loc[base['sendTime'].notna(), ['errandId','insuranceCaseId','reference','sendTime','insuranceCompanyName']]
        if send.empty:
            return pd.DataFrame(columns=self.columns)
        
        send.loc[:, ['node','source']] = ['Send_To_IC', '']
        send = send.rename(columns={
            "insuranceCaseId": "itemId",
            "sendTime": "timestamp",
            "reference": "msg",
            "insuranceCompanyName": "involved"
        }).drop_duplicates()
        
        send['timestamp'] = pd.to_datetime(send['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
        send['itemId'] = send['itemId'].apply(lambda row: 'insuranceCaseId: ' + str(row))
        send['msg'] = send['msg'].apply(lambda row: 'reference: ' + str(row))
        
        return send[self.columns]
    
    def _get_email_data(self, cond1: Any, cond2: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get email data"""
        try:
            email_base = fetchFromDB(self.log_email_query.format(COND1=cond1, COND2=cond2))
            email_base_columns = ['errandId', 'emailId', 'subject', 'textPlain', 'textHtml', 'emailTime', 'category', 'correctedCategory', 'source']

            if email_base.empty:
                return pd.DataFrame(columns=self.columns), pd.DataFrame(columns=email_base_columns)

            email_base = email_base[email_base_columns]
            # Merge text content
            email_base[['origin','email']] = email_base.apply(
                lambda row: self._merge_text(row['subject'], row['textPlain'], row['textHtml']), 
                axis=1
            ).apply(pd.Series)
            
            email = email_base.copy()
            email.loc[:, ['node']] = 'Email'
            email['correctedCategory'] = email['correctedCategory'].fillna(email['category'])
            email['correctedCategory'] = email['correctedCategory'].map(self.category_mapping)
            
            email = email.rename(columns={
                "emailId": "itemId",
                "emailTime": "timestamp",
                "email": "msg",
                "correctedCategory": "involved"
            }).drop_duplicates()
            
            email['timestamp'] = pd.to_datetime(email['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            email['itemId'] = email['itemId'].apply(lambda row: 'emailId: ' + str(row))
            
            return email[self.columns], email_base
        except:
            return pd.DataFrame(columns=self.columns), pd.DataFrame()
    
    def _get_chat_data(self, base: pd.DataFrame, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Get chat data"""
        try:
            chat = fetchFromDB(self.log_chat_query.format(COND1=cond1, COND2=cond2))
            if chat.empty:
                return pd.DataFrame(columns=self.columns)

            chat = pd.merge(chat, base, on='errandId', how='inner')
            chat.loc[:, ['node','involved','source']] = ['Chat', '', '']
            
            for idx, row in chat.iterrows():
                if pd.notna(row.get('chatDRP')):
                    chat.at[idx, 'involved'] = f"{row['chatDRP']}"
                elif pd.notna(row.get('chatClinic')):
                    chat.at[idx, 'involved'] = f"{row['clinicName']}"
                elif pd.notna(row.get('chatMessageId')):
                    chat.at[idx, 'involved'] = f"{row['insuranceCompanyName']}"
            
            chat = chat.rename(columns={
                "chatMessageId": "itemId",
                "chatTime": "timestamp",
                "message": "msg"
            }).drop_duplicates()
            
            chat['timestamp'] = pd.to_datetime(chat['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            chat['itemId'] = chat['itemId'].apply(lambda row: 'chatMessageId: ' + str(row))
            
            return chat[self.columns]
        except:
            return pd.DataFrame(columns=self.columns)
    
    def _get_comment_data(self, base: pd.DataFrame, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Get comment data"""
        try:
            comment = fetchFromDB(self.log_comment_query.format(COND1=cond1, COND2=cond2))
            if comment.empty:
                return pd.DataFrame(columns=self.columns)

            comment = pd.merge(comment, base, on='errandId', how='inner')
            comment = comment[['errandId','commentId','commentTime','commentDRP','content']]
            comment.loc[:, ['node','source']] = ['Comment', '']
            
            comment = comment.rename(columns={
                "commentId": "itemId",
                "commentTime": "timestamp",
                "commentDRP": "involved",
                "content": "msg"
            }).drop_duplicates()
            
            comment['itemId'] = comment['itemId'].apply(lambda row: 'commentId: ' + str(row))
            comment['timestamp'] = pd.to_datetime(comment['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            
            return comment[self.columns]
        except:
            return pd.DataFrame(columns=self.columns)
    
    def _create_update_entry(self, base: pd.DataFrame, email_base: pd.DataFrame) -> pd.DataFrame:
        """Create update errand record"""
        update = base.loc[
            (base['settlementAmount'].notna()) & (base['updatedTime'].notna()),
            ['errandId','insuranceCaseId','updatedTime','settlementAmount']
        ]

        if update.empty:
            return pd.DataFrame(columns=self.columns)

        # Determine update method
        involved = pd.merge(update, email_base[['errandId','category','correctedCategory']], on='errandId', how='left')
        involved.loc[involved['category'].isna(), 'involved'] = 'by Agria API'
        
        update.loc[:, ['node','source']] = ['Update_DR', '']
        update = pd.merge(update, involved[['errandId','involved']], on='errandId', how='left')
        
        update = update.rename(columns={
            "insuranceCaseId": "itemId",
            "updatedTime": "timestamp",
            "settlementAmount": "msg"
        }).drop_duplicates()
        
        update['timestamp'] = pd.to_datetime(update['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
        update['msg'] = update['msg'].apply(lambda row: f"{row} kr")
        update['itemId'] = update['itemId'].apply(lambda row: 'insuranceCaseId: ' + str(row))
        
        return update[self.columns]
    
    def _create_invoice_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create invoice records"""
        return pd.DataFrame(columns=self.columns)  # Simplified implementation
    
    def _create_payment_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create payment records"""
        return pd.DataFrame(columns=self.columns)  # Simplified implementation
    
    def _create_cancel_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create cancel records"""
        return pd.DataFrame(columns=self.columns)  # Simplified implementation

    def _create_remove_cancel_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create restore cancel records"""
        return pd.DataFrame(columns=self.columns)  # Simplified implementation
    
    def _perform_risk_assessment(self, doc: str) -> str:
        """Perform risk assessment"""
        if not self.groq_client:
            return "AI risk assessment not available"
            
        system_prompt = """
            You are an expert in log analysis and risk assessment. 
            Analyze the errand log and provide assessment in Swedish.
            """
        
        user_prompt = f"Analyze the following errand log: {doc}"

        try:
            chat_completion = self.groq_client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                model="llama-3.3-70b-versatile"
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            return f"AI analysis error: {str(e)}"
    
    def _create_chronological_log(self, base: pd.DataFrame, create: pd.DataFrame, send: pd.DataFrame, 
                                email: pd.DataFrame, chat: pd.DataFrame, comment: pd.DataFrame, 
                                update: pd.DataFrame, invoice: pd.DataFrame, payment: pd.DataFrame, 
                                cancel: pd.DataFrame, remove: pd.DataFrame) -> Tuple[Dict, Dict]:
        """Create chronological log"""
        def filter_columns(df):
            if df.empty:
                return df
            return df.loc[:, df.notna().any()]

        # Merge all data
        processed_dfs = [
            filter_columns(df) 
            for df in [create, send, email, chat, comment, update, invoice, payment, cancel, remove]
            if not df.empty
        ]

        if not processed_dfs:
            return {}, {}

        log = pd.concat(processed_dfs, ignore_index=True).drop_duplicates()
        log = pd.merge(log, base[['errandId','clinicName','insuranceCompanyName']], on='errandId', how='left')

        float_cols = log.select_dtypes(include=['float64']).columns
        if not float_cols.empty:
            log[float_cols] = log[float_cols].astype('Int64')
        
        log = log.sort_values('timestamp')
        
        grouped = log.groupby('errandId')
        group_log, group_ai = {}, {}

        for group_id, group_df in grouped:
            group_df = group_df.sort_values('timestamp').reset_index(drop=True)
            group_df['timestamp'] = group_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
            
            # Build log content
            paragraph = f"Ärenden: {group_id}\n\n"
            for idx, row in group_df.iterrows():
                timestamp = row['timestamp']
                node = row['node']
                msg = row.get('msg', '')
                involved = row.get('involved', '')
                
                if node == 'Errand_Created':
                    paragraph += f"• {timestamp} Direktregleringsärendet skapades av klinik {involved}.\n"
                elif node == 'Send_To_IC':
                    paragraph += f"• {timestamp} Direktregleringsärendet skickades till försäkringsbolag {involved}.\n"
                elif node == 'Update_DR':
                    paragraph += f"• {timestamp} Direktregleringsärendet uppdaterade ersättningsbeloppet {msg} {involved}.\n"
                elif node == 'Email':
                    paragraph += f"• {timestamp} Email: {involved} - {msg[:100]}...\n"
                elif node == 'Chat':
                    paragraph += f"• {timestamp} Chat från {involved}: {msg}\n"
                elif node == 'Comment':
                    paragraph += f"• {timestamp} Kommentar från {involved}: {msg}\n"
                else:
                    paragraph += f"• {timestamp} {node}: {msg}\n"
            
            # Format as HTML
            html_content = paragraph.replace('\n', '<br>')
            
            group_log[group_id] = {
                "title": f"Ärenden: {group_id}",
                "content": html_content
            }
            
            # AI analysis
            if self.groq_client:
                ai_analysis = self._perform_risk_assessment(paragraph)
                group_ai[group_id] = ai_analysis.replace('\n', '<br>')
            else:
                group_ai[group_id] = "AI analysis not available"
        
        return group_log, group_ai
