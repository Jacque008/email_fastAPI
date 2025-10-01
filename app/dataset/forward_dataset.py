from __future__ import annotations
import pandas as pd
from typing import Dict, Any, Optional
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
        self.journal_contact_query = self.base_service.queries['forwardJournalContact'].iloc[0]
        self.journal_contact_df = fetchFromDB(self.journal_contact_query)
        fw_cates = self.base_service.forward_suggestion[
            self.base_service.forward_suggestion['action'].str.endswith('_Template')].action.to_list()
        self.fw_cates = [item.replace('_Template', '') for item in fw_cates]


    def enrich_email_data(self) -> "ForwardDataset":
        """Enrich internal DataFrame with email data from database - returns self for chaining"""
        try:
            if not self.df.empty:
                id = self.df.iloc[0]['id']

                email_spec_query = self.base_service.queries['emailSpec'].iloc[0]
                bas_email = fetchFromDB(email_spec_query.format(COND=(f"e.id = {id}")))
                ds = EmailDataset(df=bas_email, services=self.services)
                email = ds.do_preprocess()

                email = email.copy()
                email.loc[:,'userId'] = self.df.iloc[0]['userId']
                columns_to_drop = ['receiver','errandId','reference','sender']
                email = email.drop(columns=columns_to_drop)

                forward_query = self.forward_query.format(COND=(f"e.id = {id}"))
                adds_on = fetchFromDB(forward_query)

                if not adds_on.empty:
                    category = adds_on['correctedCategory'].iloc[0]
                    if category not in self.fw_cates:
                        self.error = f"Category '{category}' is not a forwardable category"
                        return self
                    self.df = email.merge(adds_on, on='id', how='left')

        except Exception as e:
            raise Exception(f"❌ enrich_email_data error: {str(e)}")

        return self
    
    def clean_email_content(self) -> "ForwardDataset":
        """Clean email content data"""
        if hasattr(self, 'error') and self.error:
            return self
        try:
            if 'email' in self.df.columns:
                self.df['email'] = self.df['email'].str.replace(r'\n\n+', '\n', regex=True)
        except Exception as e:
            pass
        return self
    
    def _generate_forward_address(self, row_data: Dict[str, Any]) -> str:
        """Generate forward address using resolver service"""
        try:
            source = row_data.get('source', '')
            receiver = row_data.get('receiver', '')
            return self.addressResolver.detect_forward_address(source, receiver)
        except Exception:
            return ""
    
    def _generate_forward_subject(self, row_data: Dict[str, Any]) -> str:
        """Generate forward subject using generator service"""
        try:
            email = row_data.get('email', '')
            category = row_data.get('correctedCategory', '')
            return self.forward_service.generate_forwarding_subject(
                email=email,
                category=category,
                reference=row_data.get('reference', '') or '',
                sender=row_data.get('sender') or 'avsändaren'
            )
        except Exception:
            return ""
    
    def _generate_forward_content(self, row_data: Dict[str, Any]) -> str:
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

    def _generate_link_special(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Find reply address for sender - looks up sender in clinic or insurance company databases"""
        journal_data: Optional[Dict[str, Any]] = None
        action = 'Journalkopia'
        fb_journal_contact_id = None

        try:
            mask = self.journal_contact_df['insuranceCompanyId']==row_data['insuranceCompanyId']
            contact = self.journal_contact_df.loc[mask]
            if not contact.empty:
                if contact.shape[0]==1:
                    fb_journal_contact_id = contact['id'].iloc[0]
                elif pd.notna(row_data.get('kind')):
                    if row_data['insuranceCompanyId']==4: # Sveland
                        if row_data['kind'].lower() == 'häst':
                            fb_journal_contact_id = 9
                        else:
                            fb_journal_contact_id = 8
                    elif row_data['insuranceCompanyId']==5: # Agria
                        if row_data['kind'].lower() == 'häst':
                            fb_journal_contact_id = 14
                        else:
                            fb_journal_contact_id = 12
                    elif row_data['insuranceCompanyId']==7: # Dina
                        if row_data['kind'].lower() in ['hund','katt']:
                            fb_journal_contact_id = 2
                        elif row_data['kind'].lower() == 'häst':
                            fb_journal_contact_id = 3
                        else:
                            action = 'Vidarebefordra'
                            fb_journal_contact_id = None    
                else:
                    action = 'Vidarebefordra'
                    fb_journal_contact_id = None          
            else:
                action = 'Vidarebefordra'
                fb_journal_contact_id = None

            if (fb_journal_contact_id is not None):
                journal_data = {
                        'insuranceCompany': int(fb_journal_contact_id),
                        'clinic': int(row_data.get('clinicId')) if pd.notna(row_data.get('clinicId')) else None,
                        'journalNumber': str(row_data.get('journalNumber')) if pd.notna(row_data.get('journalNumber')) else None
                    }
                
            result_data = {
                        'id': int(row_data['id']),
                        'action': action,
                        'forward_address': self._generate_forward_address(row_data),
                        'forward_subject': self._generate_forward_subject(row_data),
                        'forward_text': self._generate_forward_content(row_data),
                        'journal_data': journal_data             
                    }
            
            return result_data

        except Exception as e:
            print(f"❌ Error in _generate_link_special: {str(e)}")
            return {
                'id': int(row_data.get('id', 0)),
                'action': 'Vidarebefordra',
                'forward_address': '',
                'forward_subject': '',
                'forward_text': '',
                'journal_data': None
            }

    def do_forward(self) -> pd.DataFrame:
        """Perform forwarding on the internal DataFrame - main processing method

        Returns:
            DataFrame with forwarding results
        """
        if self.df.empty:
            return pd.DataFrame([{
                'id': 0,
                'error': "No forwarding info found"
                }])

        try:
            (self.enrich_email_data()
                 .clean_email_content())

            if hasattr(self, 'error') and self.error:
                return pd.DataFrame([{
                    'id': int(self.df.iloc[0]['id']) if not self.df.empty else 0,
                    'error': self.error
                }])

            results = []
            for _, row in self.df.iterrows():
                forwarding_id = row.get('id')
                if forwarding_id is None:
                    continue

                row_data = row.to_dict()
                is_link_email = (row_data.get('correctedCategory') == 'Complement_DR_Insurance_Company'
                                 and pd.notna(row_data.get('linkJournalTenant')) 
                                 and pd.notna(row_data.get('journalNumber')))

                if is_link_email:
                    result_data = self._generate_link_special(row_data)
                    
                else:
                    result_data = {
                        'id': int(forwarding_id),
                        'action': 'Vidarebefordra',
                        'forward_address': self._generate_forward_address(row_data),
                        'forward_subject': self._generate_forward_subject(row_data),
                        'forward_text': self._generate_forward_content(row_data),
                        'journal_data': None
                    }

                results.append(result_data)

            return pd.DataFrame(results)

        except Exception as e:
            raise Exception(f"Forwarding processing failed: {str(e)}")

