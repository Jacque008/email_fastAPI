from __future__ import annotations
import pandas as pd
from typing import Dict, Any
from dataclasses import dataclass, field
from ..services.utils import fetchFromDB
from ..services.services import DefaultServices
from ..services.base_service import BaseService
from ..services.forward import ForwardService
from ..services.resolver import AddressResolver
from .email_dataset import EmailDataset

@dataclass
class ForwardDataset:
    """Dataset for email forwarding - follows DataFrame-first pattern like PaymentDataset"""
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    services: DefaultServices = field(default_factory=DefaultServices)
    
    base_service: BaseService = field(init=False)
    forward_service: ForwardService = field(init=False)
    addressResolver: AddressResolver = field(init=False)
    forward_query: str = field(init=False)

    def __post_init__(self):
        """Initialize all services and queries after dataclass creation"""
        self.base_service = BaseService()
        self.forward_service = self.services.get_forwarder()
        self.addressResolver = self.services.get_addressResolver()
        self.forward_query = self.base_service.queries['forwardSummaryInfo'].iloc[0]
    
    def enrich_email_data(self) -> "ForwardDataset":
        """Enrich internal DataFrame with email data from database - returns self for chaining"""
        try:
            if not self.df.empty:
                id = self.df.iloc[0]['id']

                # Use pre-initialized queries and base service
                forward_query = self.forward_query
                email_spec_query = self.base_service.queries['emailSpec'].iloc[0]

                bas_email = fetchFromDB(email_spec_query.format(EMAILID=id))
                ds = EmailDataset(df=bas_email, services=self.services)
                email = ds.do_preprocess()

                email = email.copy()
                email.loc[:,'userId'] = self.df.iloc[0]['userId']
                columns_to_drop = ['receiver','errandId','reference','sender']
                existing_columns = [col for col in columns_to_drop if col in email.columns]
                if existing_columns:
                    email = email.drop(columns=existing_columns)

                adds_on = fetchFromDB(forward_query.format(ID=id))

                if not adds_on.empty:
                    self.df = email.merge(adds_on, on='id', how='left')

        except Exception as e:
            print(f"❌ enrich_email_data error: {str(e)}")
            raise
        return self
    
    def clean_email_content(self) -> "ForwardDataset":
        """Clean email content data"""
        try:
            if 'email' in self.df.columns:
                self.df['email'] = self.df['email'].str.replace(r'\n\n+', '\n', regex=True)
        except Exception as e:
            pass
        return self
    
    def _generate_forward_address_data(self, row_data: Dict[str, Any]) -> str:
        """Generate forward address using resolver service"""
        try:
            source = row_data.get('source', '')
            receiver = row_data.get('receiver', '')
            return self.addressResolver.detect_forward_address(source, receiver)
        except Exception:
            return ""
    
    def _generate_forward_subject_data(self, row_data: Dict[str, Any]) -> str:
        """Generate forward subject using generator service"""
        try:
            email = row_data.get('email', '')
            category = row_data.get('correctedCategory', '')
            return self.forward_service.generate_forwarding_subject(
                email=email,
                category=category,
                reference=row_data.get('reference', ''),
                sender=row_data.get('sender', '')
            )
        except Exception:
            return ""
    
    def _generate_forward_content_data(self, row_data: Dict[str, Any]) -> str:
        """Generate forward content using generator service"""
        try:
            user_id = row_data.get('userId', '')
            admin_name = self.addressResolver.resolve_admin_details(user_id)
            return self.forward_service.generate_email_content(
                row_data=row_data,
                admin_name=admin_name.result['adminInfo']
            )
        except Exception:
            return ""
    
    def do_forward(self) -> pd.DataFrame:
        """Perform forwarding on the internal DataFrame - main processing method

        Returns:
            DataFrame with forwarding results
        """
        if self.df.empty:
            return pd.DataFrame()

        try:
            (self.enrich_email_data()
                .clean_email_content())
            
            results = []
            for _, row in self.df.iterrows():
                forwarding_id = row.get('id')
                if forwarding_id is None:
                    continue

                row_data = row.to_dict()
                result_data = {
                    'id': int(forwarding_id),
                    'forward_address': self._generate_forward_address_data(row_data),
                    'forward_subject': self._generate_forward_subject_data(row_data),
                    'forward_text': self._generate_forward_content_data(row_data)
                }

                results.append(result_data)

            return pd.DataFrame(results)

        except Exception as e:
            raise Exception(f"Forwarding processing failed: {str(e)}")

