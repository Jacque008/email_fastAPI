from __future__ import annotations
import pandas as pd
from typing import Optional, Dict, List
from dataclasses import dataclass, field
from ..services.utils import fetchFromDB
from ..services.services import DefaultServices
from ..services.base_service import BaseService
from ..schemas.forward import ForwardingIn, ForwardingOut
from .email_dataset import EmailDataset

@dataclass
class ForwardingEmailDataset(BaseService):
    services: DefaultServices = field(default_factory=DefaultServices)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    
    def __post_init__(self):
        super().__init__()
        self.forwarder = self.services.get_forwarder()
        self.addressResolver = self.services.get_addressResolver()
        self._setup_generation_configs()

    
    def _setup_generation_configs(self):
        self.forward_query = self.queries['forwardSummaryInfo'].iloc[0]
        
    def init_fw_email(self, request: ForwardingIn) -> "ForwardingEmailDataset":
        """Initialize DataFrame with request data"""
        self.df = pd.DataFrame([{
            'id': request.id,
            'userId': request.userId
        }])

        return self
    
    def enrich_email_data(self) -> "ForwardingEmailDataset":
        """Enrich DataFrame with email data from database"""
        try:
            if self.forward_query and not self.df.empty:
                id = self.df.iloc[0]['id']

                bas_email = fetchFromDB(self.email_spec_query.format(EMAILID=id))
                ds = EmailDataset(df=bas_email, services=self.services)
                email = ds.do_preprocess()
                email = email.copy()
                email.loc[:,'userId'] = self.df.iloc[0]['userId']
                columns_to_drop = ['receiver','errandId','reference','sender'] 
                existing_columns = [col for col in columns_to_drop if col in email.columns]
                if existing_columns:
                    email = email.drop(columns=existing_columns)
                    
                adds_on = fetchFromDB(self.forward_query.format(ID=id))          
                if not adds_on.empty:
                    self.df = email.merge(adds_on, on='id', how='left')
                    
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
        required_fields = ['id', 'receiver', 'correctedCategory']
        for field in required_fields:
            if field not in self.df.columns or self.df[field].isna().all():
                print(f"Missing required field: {field}")
                self.df = pd.DataFrame()  
                break

        return self
    
    def generate_forward_address(self, result: ForwardingOut, 
                                row_data: Dict) -> ForwardingOut:
        """Generate forward address using resolver service"""
        try:
            source = row_data.get('source', '')
            receiver = row_data.get('receiver', '')
            result.forward_address = self.addressResolver.detect_forward_address(source, receiver)
        except Exception as e:
            print(f"Failed to generate forward address: {str(e)}")
            result.forward_address = ""
        return result
    
    def generate_forward_subject(self, result: ForwardingOut, 
                                row_data: Dict) -> ForwardingOut:
        """Generate forward subject using generator service"""
        try:
            email = row_data.get('email', '')
            category = row_data.get('correctedCategory', '')
            result.forward_subject = self.forwarder.generate_forwarding_subject(
                email=email,
                category=category,
                reference=row_data.get('reference', ''),
                sender=row_data.get('sender', '')
            )
        except Exception as e:
            print(f"Failed to generate forward subject: {str(e)}")
            result.forward_subject = ""
        
        return result
    
    def generate_forward_content(self, result: ForwardingOut, row_data: Dict) -> ForwardingOut:
        """Generate forward content using generator service"""
        try:
            user_id = row_data.get('userId', '')
            admin_name = self.addressResolver.resolve_admin_details(user_id)
            result.forward_text = self.forwarder.generate_email_content(
                row_data=row_data,
                admin_name=admin_name.result['adminInfo']
            )
        except Exception as e:
            print(f"Failed to generate forward content: {str(e)}")
            result.forward_text = ""
        
        return result
    

    def do_forwarding(self, request: ForwardingIn) -> ForwardingOut:
        try:
            ds = ForwardingEmailDataset(services=self.services)
            ds.init_fw_email(request)\
              .enrich_email_data()\
              .clean_email_content()#\
            #   .validate_data()
            
            result = ForwardingOut(id=request.id)
            if ds.df.empty:
                # result.error_message = "Failed to retrieve email data"
                return result

            row_data = ds.df.iloc[0].to_dict()

            result = ds.generate_forward_address(result, row_data)
            result = ds.generate_forward_subject(result, row_data)
            result = ds.generate_forward_content(result, row_data)
            return result
            
        except Exception as e:
            raise Exception(f"Forwarding processing failed: {str(e)}")

    

    