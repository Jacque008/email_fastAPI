"""
Email Service - Handle email summary and forwarding functionality
Based on original llmSummary.py and createForwarding.py
"""
import re
import os
import groq
import regex as reg
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from bs4 import BeautifulSoup
from html import escape
from groq import Groq
from pydantic import BaseModel

from ..app.services.base_service import BaseService
from ..app.services.utils import fetchFromDB
from ..app.workflow.create_forwarding import process_single_forwarding


class EmailSummaryRequest(BaseModel):
    emailId: Optional[int] = None
    errandNumber: Optional[str] = None
    reference: Optional[str] = None


class EmailForwardingIn(BaseModel):
    id: int
    recipient: str
    correctedCategory: str
    userId: Optional[int] = None


class EmailSummaryResponse(BaseModel):
    error_message: Optional[str] = None
    summary_chat_with_clinic: Optional[str] = None
    summary_chat_with_ic: Optional[str] = None
    summary_email_conversation: Optional[str] = None


class EmailForwardingResponse(BaseModel):
    id: int
    forwardAddress: str
    forwardSubject: str
    forwardText: str


class EmailService(BaseService):
    """Email service class for handling email summary and forwarding"""
    
    def __init__(self):
        super().__init__()
        self.model = self._load_model()
        self.forward_format = self._load_forward_format()
        self.groq_client = self._initialize_groq_client()
        self.system_prompt = self._get_system_prompt()
        self._setup_forwarding_data()
    
    def _load_model(self) -> str:
        """Load AI model configuration"""
        try:
            model_df = pd.read_csv(f"{self.folder}/model.csv")
            return model_df['model'].iloc[0]
        except:
            return "llama-3.3-70b-versatile"  # Default model
    
    def _load_forward_format(self) -> pd.DataFrame:
        """Load forwarding format configuration"""
        try:
            return pd.read_csv(f"{self.folder}/forwardFormat.csv")
        except:
            return pd.DataFrame()
    
    def _initialize_groq_client(self) -> Groq:
        """Initialize Groq client"""
        api_key = os.getenv('GROQ_API_KEY')
        return Groq(api_key=api_key)
    
    def _get_system_prompt(self) -> Dict[str, str]:
        """Get system prompts"""
        return {
            "role": "system",
            "content": (
                "You are a professional and efficient assistant from Sweden, "
                "specializing in accurately summarizing conversations in Swedish. "
                "Your summaries must be clear, concise, accurate, and well-structured. "
                "Format the summary as bullet points, starting each point with '*   '. "
                "Ensure precision and avoid unnecessary information."
                "Do not translate the text."
                "\n\n"
                "Background information: "
                "- 'Clinic' refers to veterinary clinics that provide medical care for pets. After treating a pet, clinics are responsible for submitting claims to insurance companies, providing accurate and complete information required for reimbursement. "
                "- 'Insurance company' refers to the entity responsible for processing claims submitted by clinics. They request the necessary information, make claim decisions, and ensure timely payments. "
                "- 'DRP' (direktreglering company) acts as an intermediary platform connecting clinics and insurance companies. All communication between clinics and insurance companies occurs via the DRP platform. DRP is also responsible for forwarding emails and handling payments: insurance companies pay DRP, which deducts service fees before forwarding payments to clinics."
            )
        }
    
    def _setup_forwarding_data(self):
        """Setup forwarding related data"""
        try:
            # Get forwarding suggestions and other configurations from preprocessed data
            self.trun_list = self.forward_sugg[self.forward_sugg['action']=='Trim'].templates.to_list()
            self.forw_sub_list = self.forward_sugg[self.forward_sugg['action']=='Forward_Subject'].templates.to_list()
            self.sub_list = self.forward_sugg[self.forward_sugg['action']=='Subject'].templates.to_list()
            self.ic_forw_add = self.ic[:17].set_index('insuranceCompany')['forwardAddress'].to_dict()
            self.clinic_forw_add = self.clinic.loc[
                self.clinic['role'] == 'main_email', 
                ['clinicName','clinicEmail']
            ].drop_duplicates()
            self.forward_summary_info_query = self.queries['forwardSummaryInfo'].iloc[0]
        except Exception as e:
            # Set default values
            self.trun_list = []
            self.forw_sub_list = []
            self.sub_list = []
            self.ic_forw_add = {}
            self.clinic_forw_add = pd.DataFrame()
            self.forward_summary_info_query = ""
    
    # =========================
    # Email Summary Functions
    # =========================
    
    def _fetch_data(self, kind: str, condition: str) -> pd.DataFrame:
        """Fetch data: chat records, emails or comments"""
        query_mapping = {
            'chat': 'summaryChat',
            'email': 'summaryEmail', 
            'comment': 'summaryComment'
        }
        
        if kind not in query_mapping:
            return pd.DataFrame()
        
        try:
            query = self.queries[query_mapping[kind]].iloc[0].format(CONDITION=condition)
            return fetchFromDB(query)
        except:
            return pd.DataFrame()
    
    def generate_summary(self, request: EmailSummaryRequest, use_case: str = 'api') -> Dict[str, Any]:
        """Generate email summary"""
        # Build query conditions
        condition = {}
        if request.emailId:
            condition['chat'] = f"e.id = {request.emailId}"
            condition['email'] = f"e.id = {request.emailId}"
            condition['comment'] = f"e.id = {request.emailId}"
        elif request.errandNumber:
            condition['chat'] = f"e.errandNumber = '{request.errandNumber}'"
            condition['email'] = f"e.errandNumber = '{request.errandNumber}'"
            condition['comment'] = f"e.errandNumber = '{request.errandNumber}'"
        elif request.reference:
            condition['chat'] = f"ic.reference = '{request.reference}'"
            condition['email'] = f"ic.reference = '{request.reference}'"
            condition['comment'] = f"ic.reference = '{request.reference}'"
        else:
            return {"error_message": "Missing required parameters"}
        
        # Get and process data
        chat = self._fetch_data('chat', condition['chat'])
        email = self._fetch_data('email', condition['email'])
        comment = self._fetch_data('comment', condition['comment'])
        
        # Merge processing
        combine = self._process_combine(chat, email, comment)
        summary_combine, error_combine = None, None
        
        if not combine.empty and combine['content'].notna().any():
            msg_combine = self._format_combine(combine)
            if msg_combine:
                summary_combine, error_combine = self._get_ai_response(msg_combine)
        else:
            error_combine = "No available data"
        
        return {
            'summary_combine': summary_combine,
            'error_combine': error_combine
        }
    
    def _process_combine(self, chat: pd.DataFrame, email: pd.DataFrame, comment: pd.DataFrame) -> pd.DataFrame:
        """Merge chat, email and comment data"""
        cols = ['createdAt', 'source', 'content', 'combineType']
        
        # Process chat data
        if not chat.empty:
            chat = chat.rename({'message':'content'}, axis=1)
            chat["combineType"] = "chat"
        else:
            chat = pd.DataFrame(columns=cols)
        
        # Process email data    
        if not email.empty:
            email = email.rename({'email':'content'}, axis=1)
            email["combineType"] = "email"
        else:
            email = pd.DataFrame(columns=cols)
        
        # Process comment data
        if not comment.empty:
            comment["combineType"] = "comment"
        else:
            comment = pd.DataFrame(columns=cols)
        
        # Merge all non-empty data
        no_empty = [df for df in [chat[cols], email[cols], comment[cols]] if not df.empty]
        if no_empty:
            data = pd.concat(no_empty, ignore_index=True).sort_values(by='createdAt', ignore_index=True)
        else:
            data = pd.DataFrame(columns=cols)
        
        return data
    
    def _format_combine(self, combine: pd.DataFrame) -> List[Dict]:
        """Format combined data for AI processing"""
        user_prompt = (
            "Summarize the following conversation between the clinic, insurance company, and DRP concisely and accurately in Swedish. "
            "Capture only the key points from the email conversations and comments. "
            "Replace 'insurance company' with 'FB' in all instances. "
            "\n\n"
            "Additional instructions: "
            "- If DRP only forwarded an email, do not mention it in the summary to save space. "
            "- A forwarded email is defined as an email where most of the content is identical to the previous email, with only some template text added at the beginning or end. "
            "- If DRP performed any actions beyond forwarding emails, include those actions in the summary. "
            "- Ensure the summary is concise, well-structured, and focuses on key points relevant to the errand."
            "\n\n"
            "{COMBINE}")
        
        msg_combine, his_combine = [], []
        if not combine.empty:
            for _, row in combine.iterrows():
                his_combine.append({
                    "role": row['combineType'] + " from " + row['source'] if pd.notna(row['source']) and (row['source'] in ["Clinic", "Insurance_Company", "DRP"]) else "Other",
                    "content": row['content'].strip()
                })
            
            combine_text = "\n".join([f"{item['role']}: {item['content']}" for item in his_combine])
            
            msg_combine.append(self.system_prompt)
            msg_combine.append({
                "role": "user",
                "content": user_prompt.format(COMBINE=combine_text)
            })
        
        return msg_combine
    
    def _get_ai_response(self, msg: List[Dict]) -> Tuple[Optional[str], Optional[str]]:
        """Get AI response"""
        summary, error_message = None, None
        try:
            chat_completion = self.groq_client.chat.completions.create(
                messages=msg,
                model=self.model
            )
            summary = chat_completion.choices[0].message.content
            parts = re.split(r':\s*', summary)
            if len(parts) > 1:
                summary = ":".join(parts[1:])
        except groq.APIConnectionError:
            error_message = "The server could not be reached. Please try again later."
        except groq.RateLimitError:
            error_message = "Rate limit exceeded. Please try again later."
        except groq.APIStatusError as e:
            error_message = f"API Error: {e.status_code}, Response: {e.response}"
        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
        
        return summary, error_message
    
    # =========================
    # Email Forwarding Functions  
    # =========================
    
    def generate_forwarding(self, request: EmailForwardingIn) -> EmailForwardingResponse:
        """Generate email forwarding using new workflow"""
        try:
            # Use the new dataclass-based workflow
            result = process_single_forwarding(
                email_id=request.id,
                recipient=request.recipient,
                corrected_category=request.correctedCategory,
                user_id=request.userId
            )
            
            return EmailForwardingResponse(
                id=result['id'],
                forwardAddress=result['forwardAddress'],
                forwardSubject=result['forwardSubject'],
                forwardText=result['forwardText']
            )
            
        except Exception as e:
            # Return empty response with error indication
            return EmailForwardingResponse(
                id=request.id,
                forwardAddress="",
                forwardSubject=f"Error: {str(e)}",
                forwardText="Failed to generate forwarding content"
            )
    
    # Legacy method - kept for compatibility
    def _create_forwarding(self, row: pd.Series) -> Tuple[str, str]:
        """Legacy forwarding method - now redirects to new workflow"""
        print("Warning: Using legacy _create_forwarding method. Consider using new workflow directly.")
        return "Legacy Subject", "Legacy Content"
