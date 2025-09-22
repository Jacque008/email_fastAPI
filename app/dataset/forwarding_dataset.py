from __future__ import annotations
import pandas as pd
import time
from typing import Dict
from dataclasses import dataclass, field
from ..services.utils import fetchFromDB, model_to_dataframe
from ..services.services import DefaultServices
from ..services.base_service import BaseService
from ..schemas.forward import ForwardingIn, ForwardingOut
from .email_dataset import EmailDataset

@dataclass
class ForwardingEmailDataset(BaseService):
    services: DefaultServices = field(default_factory=DefaultServices)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    
    def __post_init__(self):
        start_time = time.time()

        super().__init__()

        total_time = time.time() - start_time

    @property
    def forwarder(self):
        if not hasattr(self, '_forwarder'):
            step_start = time.time()
            self._forwarder = self.services.get_forwarder()
        return self._forwarder

    @property
    def addressResolver(self):
        if not hasattr(self, '_addressResolver'):
            step_start = time.time()
            self._addressResolver = self.services.get_addressResolver()
        return self._addressResolver

    @property
    def forward_query(self):
        if not hasattr(self, '_forward_query'):
            self._forward_query = self.queries['forwardSummaryInfo'].iloc[0]
        return self._forward_query
    
    def enrich_email_data(self, request: ForwardingIn) -> "ForwardingEmailDataset":
        """Enrich DataFrame with email data from database"""
        try:
            step_start = time.time()
            self.df = model_to_dataframe(request)

            if self.forward_query and not self.df.empty:
                id = self.df.iloc[0]['id']

                step_start = time.time()
                bas_email = fetchFromDB(self.email_spec_query.format(EMAILID=id))

                step_start = time.time()
                ds = EmailDataset(df=bas_email, services=self.services)

                step_start = time.time()
                email = ds.do_preprocess()

                email = email.copy()
                email.loc[:,'userId'] = self.df.iloc[0]['userId']
                columns_to_drop = ['receiver','errandId','reference','sender']
                existing_columns = [col for col in columns_to_drop if col in email.columns]
                if existing_columns:
                    email = email.drop(columns=existing_columns)

                step_start = time.time()
                adds_on = fetchFromDB(self.forward_query.format(ID=id))

                if not adds_on.empty:
                    self.df = email.merge(adds_on, on='id', how='left')

        except Exception as e:
            print(f"❌ enrich_email_data error: {str(e)}")
            import traceback
            print(f"❌ enrich_email_data traceback: {traceback.format_exc()}")
        return self
    
    def clean_email_content(self) -> "ForwardingEmailDataset":
        """Clean email content data"""
        try:
            if 'email' in self.df.columns:
                self.df['email'] = self.df['email'].str.replace(r'\n\n+', '\n', regex=True)
        except Exception as e:
            pass
        return self
    
    def generate_forward_address(self, result: ForwardingOut, 
                                row_data: Dict) -> ForwardingOut:
        """Generate forward address using resolver service"""
        try:
            source = row_data.get('source', '')
            receiver = row_data.get('receiver', '')
            result.forward_address = self.addressResolver.detect_forward_address(source, receiver)
        except Exception as e:
            pass
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
            pass
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
            pass
            result.forward_text = ""
        
        return result
    
    def do_forwarding(self, request: ForwardingIn) -> ForwardingOut:
        try:
            start_time = time.time()

            # Use self instead of creating a new instance (avoiding double initialization)
            step_start = time.time()
            self.enrich_email_data(request)

            step_start = time.time()
            self.clean_email_content()

            result = ForwardingOut(id=request.id)
            if self.df.empty:
                print("❌ DataFrame is empty, returning empty result")
                return result

            row_data = self.df.iloc[0].to_dict()

            step_start = time.time()
            result = self.generate_forward_address(result, row_data)

            step_start = time.time()
            result = self.generate_forward_subject(result, row_data)

            step_start = time.time()
            result = self.generate_forward_content(result, row_data)

            total_time = time.time() - start_time
            return result

        except Exception as e:
            import traceback
            print(f"❌ do_forwarding: Error occurred - {str(e)}")
            print(f"❌ Full traceback: {traceback.format_exc()}")
            raise Exception(f"Forwarding processing failed: {str(e)}")

    

    