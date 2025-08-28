"""
Address service - Handle forwarding address management
"""
import pandas as pd
from ..app.services.base_service import BaseService

class AddressService(BaseService):
    """Address service"""
    
    def __init__(self):
        super().__init__()
        self._load_address_mappings()
    
    def _load_address_mappings(self):
        """Load address mappings"""
        try:
            self.ic_addresses = self.fb[:17].set_index('insuranceCompany')['forwardAddress'].to_dict()
            self.clinic_addresses = self.clinic_list.loc[
                self.clinic_list['role'] == 'main_email', 
                ['clinicName', 'clinicEmail']
            ].drop_duplicates()
        except:
            self.ic_addresses = {}
            self.clinic_addresses = pd.DataFrame()
    
    def get_insurance_company_address(self, company_name: str) -> str:
        """Get insurance company forwarding address"""
        return self.ic_addresses.get(company_name, "")
    
    def get_clinic_address(self, clinic_name: str) -> str:
        """Get clinic forwarding address"""
        clinic_row = self.clinic_addresses[
            self.clinic_addresses['clinicName'] == clinic_name
        ]
        return clinic_row.iloc[0]['clinicEmail'] if not clinic_row.empty else ""
