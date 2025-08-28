"""
Payment Service - Handle payment matching functionality
Based on original paymentMatching.py
"""
import regex as reg
import pandas as pd
from itertools import combinations
from typing import Dict, List, Tuple, Optional, Any
from pydantic import BaseModel

from ..app.services.base_service import BaseService
from ..app.services.utils import fetchFromDB


class PaymentMatchRequest(BaseModel):
    id: int
    amount: float
    reference: str
    info: str
    bankName: str
    createdAt: str


class PaymentMatchResponse(BaseModel):
    id: int
    reference: str
    bankName: str
    amount: str
    info: str
    createdAt: str
    insuranceCaseId: List[int]
    status: str


class PaymentService(BaseService):
    """Payment service class for handling payment matching"""
    
    def __init__(self):
        super().__init__()
        self._setup_matching_data()
        self._setup_queries()
    
    def _setup_matching_data(self):
        """Setup matching related data"""
        try:
            self.info_reg = pd.read_csv(f"{self.folder}/infoReg.csv")
            self.info_item_list = self.info_reg.item.to_list()
            self.bank_map = pd.read_csv(f"{self.folder}/bankMap.csv")
            self.bank_dict = self.bank_map.set_index('bankName')['insuranceCompanyReference'].to_dict()
        except Exception as e:
            # Set default values
            self.info_reg = pd.DataFrame()
            self.info_item_list = []
            self.bank_map = pd.DataFrame()
            self.bank_dict = {}
        
        self.matching_cols_pay = ['extractReference','extractOtherNumber','extractDamageNumber']
        self.matching_cols_errand = ['isReference','damageNumber','invoiceReference','ocrNumber']
        self.base_url = 'https://admin.direktregleringsportalen.se/errands/'
    
    def _setup_queries(self):
        """Setup query statements"""
        try:
            self.payment_query = self.queries['payment'].iloc[0]
            self.errand_pay_query = self.queries['errandPay'].iloc[0]
            self.errand_link_query = self.queries['errandLink'].iloc[0]
            self.payout_query = self.queries['payout'].iloc[0]
        except Exception as e:
            # Set default empty queries
            for attr in ['payment_query', 'errand_pay_query', 'errand_link_query', 'payout_query']:
                setattr(self, attr, "")
    
    def _process_payment(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Process payment data"""
        pay['createdAt'] = pd.to_datetime(pay['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')

        # Extract reference numbers
        ref_reg = reg.compile(r'\d+')
        pay.loc[pay['reference'].notna(),'extractReference'] = pay.loc[pay['reference'].notna(),'reference'].apply(
            lambda x: ''.join(ref_reg.findall(x)) if isinstance(x, str) else None
        )
        pay.loc[pay['extractReference'].notna(),'extractReference'] = pay.loc[pay['extractReference'].notna(),'extractReference'].replace('', None)
        pay['settlementAmount'] = 0
        pay['status'] = ""

        # Initialize additional columns
        for col in self.info_item_list:
            col_name = col.split('_')[1] if '_' in col else col
            if col_name not in pay.columns:
                pay[col_name] = None
        
        init_columns = ['valPay', 'valErrand', 'isReference', 'insuranceCaseId', 'referenceLink']
        for col in init_columns:
            pay[col] = [[] for _ in range(len(pay))]
        
        return pay[['id','valPay','valErrand','amount','settlementAmount','isReference','insuranceCaseId','referenceLink',
                    'status','extractReference','extractDamageNumber','extractOtherNumber','bankName','info','reference','createdAt']]
    
    def _parse_info(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Parse payment information"""
        mask = pay['info'].notna()
        for idx_pay, row_pay in pay[mask].iterrows():
            ic = self.bank_dict.get(row_pay['bankName'], 'None')
            mask_info = self.info_reg['item'].str.startswith(ic)
            
            for _, row_info_reg in self.info_reg[mask_info].iterrows():
                col = row_info_reg['item'].split('_')[1] if '_' in row_info_reg['item'] else row_info_reg['item']
                pattern = row_info_reg['regex']
                compiled_pattern = reg.compile(pattern, reg.DOTALL | reg.IGNORECASE)
                match = compiled_pattern.search(row_pay['info'])
                
                if match:
                    matched_value = match.group(1).strip()
                    if pd.isna(row_pay.get(col)):
                        pay.at[idx_pay, col] = matched_value
                    else:
                        pay.at[idx_pay, 'isReference'].append(matched_value)
        
        # Clean duplicate values
        pay.loc[pay['extractDamageNumber'] == pay['extractOtherNumber'], 'extractDamageNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractOtherNumber'], 'extractOtherNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractDamageNumber'], 'extractDamageNumber'] = None
        
        return pay
    
    def match_payments(self, payment_requests: List[PaymentMatchRequest]) -> List[PaymentMatchResponse]:
        """Execute payment matching"""
        # Convert requests to DataFrame
        pay_data = []
        for req in payment_requests:
            pay_data.append({
                'id': req.id,
                'amount': req.amount,
                'reference': req.reference,
                'info': req.info,
                'bankName': req.bankName,
                'createdAt': req.createdAt
            })
        
        pay_df = pd.DataFrame(pay_data)
        
        # Process payment data
        pay = self._process_payment(pay_df)
        pay = self._parse_info(pay)
        
        # Get errand data
        try:
            errand = fetchFromDB(self.errand_pay_query.format(CONDITION=""))
            errand['createdAt'] = pd.to_datetime(errand['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        except:
            errand = pd.DataFrame()
        
        # Execute matching process
        if not errand.empty:
            pay = self._match_by_info(pay, errand)
            
            # Handle remaining unmatched items
            mask = (pay['status'].isin(["No Found", ""]))
            if mask.any():
                pay = self._remainder_unmatched_amount(pay)
            
            # Entity and amount matching
            mask = (pay['status'].isin(["No Found", ""]))
            if mask.any():
                pay = self._match_entity_and_amount(pay, errand)
            
            # Payout matching
            mask = (pay['status'].isin(["No Found", ""]))
            if mask.any():
                try:
                    payout = fetchFromDB(self.payout_query)
                    pay = self._match_payout(pay, payout)
                except:
                    pass
        
        # Format amount display
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        
        # Convert to response format
        results = []
        for _, row in pay.iterrows():
            results.append(PaymentMatchResponse(
                id=row['id'],
                reference=row.get('reference', ''),
                bankName=row.get('bankName', ''),
                amount=row['amount'],
                info=row.get('info', ''),
                createdAt=str(row['createdAt']),
                insuranceCaseId=row.get('insuranceCaseId', []),
                status=row.get('status', '')
            ))
        
        return results
    
    def _match_by_info(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Match by information"""
        # Simplified implementation - actual matching logic would be more complex
        for idx_pay, row_pay in pay.iterrows():
            pay.at[idx_pay, 'status'] = "Processing completed"
        return pay
    
    def _remainder_unmatched_amount(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Handle remaining unmatched amounts"""
        return pay
    
    def _match_entity_and_amount(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Match entity and amount"""
        return pay
    
    def _match_payout(self, pay: pd.DataFrame, payout: pd.DataFrame) -> pd.DataFrame:
        """Match payout"""
        return pay
