from __future__ import annotations
import pandas as pd
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from ..services.utils import fetchFromDB
from ..services.services import DefaultServices
from ..services.base_service import BaseService
from ..schemas.fw_email import ForwardingIn, ForwardingOut

@dataclass
class ForwardingEmailDataset(BaseService):
    services: DefaultServices = field(default_factory=DefaultServices)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)

    def __post_init__(self):
        self.forwarder = self.services.get_forwarder()
        self.addressResolver = self.services.get_addressResolver()
        self._setup_generation_configs()
    
    def _setup_generation_configs(self):
        fw_cates = self.forward_suggestion[self.forward_suggestion['action'].str.endswith('_Template')].action.to_list()
        self.fw_cates = [item.replace('_Template', '') for item in fw_cates]
        self.forward_format = pd.read_csv(f"{self.folder}/forwardFormat.csv")
        self.trun_list = self.forward_suggestion[self.forward_suggestion['action']=='Trim'].templates.to_list()
        self.request_fw_sub = self.forward_suggestion[self.forward_suggestion['action']=='Forward_Subject'].templates.to_list()
        self.sub_list = self.forward_suggestion[self.forward_suggestion['action']=='Subject'].templates.to_list()
        self.forward_query = self.queries.get('forwardSummaryInfo', None)

    def initialize_dataframe(self, request: ForwardingIn) -> "ForwardingEmailDataset":
        """Initialize DataFrame with request data"""
        self.df = pd.DataFrame([{
            'id': request.email_id,
            'recipient': request.recipient,
            'correctedCategory': request.corrected_category,
            'userId': request.user_id
        }])
        return self
    
    def enrich_with_email_data(self) -> "ForwardingEmailDataset":
        """Enrich DataFrame with email data from database"""
        try:
            if self.forward_query and not self.df.empty:
                email_id = self.df.iloc[0]['id']
                email_data = fetchFromDB(self.forward_query.format(ID=email_id))               
                if not email_data.empty:
                    self.df = pd.merge(self.df, email_data, left_on='id', right_on='id', how='left')
        except Exception as e:
            print(f"Failed to enrich with email data: {str(e)}")
        return self
    
    def clean_email_content(self) -> "ForwardingEmailDataset":
        """Clean email content data"""
        try:
            if 'email' in self.df.columns:
                self.df['email'] = self.df['email'].str.replace(r'\n\n+', '\n', regex=True)
        except Exception as e:
            print(f"Failed to clean email content: {str(e)}")
        return self
    
    def validate_data(self)-> "ForwardingEmailDataset":
        """Validate required data is present"""
        if self.df.empty:
            return self
        required_fields = ['id', 'recipient', 'correctedCategory']
        for field in required_fields:
            if field not in self.df.columns or self.df[field].isna().all():
                print(f"Missing required field: {field}")
                self.df = pd.DataFrame()  
                break
        return self
    
    def generate_forward_address(self, result: ForwardingOut, 
                                row_data: Dict, recipient: str) -> ForwardingOut:
        """Generate forward address using resolver service"""
        try:
            source = row_data.get('source', '')
            result.forward_address = self.addressResolver.detect_forward_address(source, recipient)
        except Exception as e:
            print(f"Failed to generate forward address: {str(e)}")
            result.forward_address = ""
        return result
    
    def generate_forward_subject(self, result: ForwardingOut, 
                                row_data: Dict, category: str) -> ForwardingOut:
        """Generate forward subject using generator service"""
        try:
            email_content = row_data.get('email', '')
            result.forward_subject = self.forwarder.generate_forwarding_subject(
                email_content=email_content,
                category=category,
                reference=row_data.get('reference', ''),
                sender=row_data.get('sender', '')
            )
        except Exception as e:
            print(f"Failed to generate forward subject: {str(e)}")
            result.forward_subject = ""
        
        return result
    
    def generate_forward_content(self, result: ForwardingOut, row_data: Dict, 
                                category: str, user_id: Optional[int]) -> ForwardingOut:
        """Generate forward content using generator service"""
        try:
            admin_name = self.addressResolver.resolve_admin_details(user_id)
            result.forward_text = self.forwarder.generate_email_content(
                row_data=row_data,
                category=category,
                admin_name=admin_name
            )
        except Exception as e:
            print(f"Failed to generate forward content: {str(e)}")
            result.forward_text = ""
        
        return result
    
    @staticmethod
    def finalize_result(result: ForwardingOut) -> ForwardingOut:
        result.success = bool(result.forward_address and result.forward_subject and result.forward_text)
        if not result.success and not result.error_message:
            result.error_message = "Failed to generate complete forwarding content"
        return result
    

    