import regex as reg
import pandas as pd
import time
from typing import Dict, Any
from .base_service import BaseService
from .utils import get_payoutEntity, fetchFromDB


class PaymentServiceOptimized(BaseService):
    """Highly optimized payment service - fully vectorized approach"""
    
    def __init__(self):
        super().__init__()
        self.infoReg = pd.read_csv(f"{self.folder}/infoReg.csv")
        self.infoItemList = self.infoReg.item.to_list()
        self.bankMap = pd.read_csv(f"{self.folder}/bankMap.csv")
        self.bankDict = self.bankMap.set_index('bankName')['insuranceCompanyReference'].to_dict()
        
        # Create reverse mapping from reference to bank name for proper filtering
        self.refToBankDict = self.bankMap.set_index('insuranceCompanyReference')['bankName'].to_dict()
        
        # Create mapping from reference to likely insurance company name in errand data
        self.refToInsuranceNameDict = {
            'agria': 'Agria',
            'dina': 'Dina Försäkringar', 
            'folksam': 'Folksam',
            'if': 'If',
            'sveland': 'Sveland',
            'trygghansa': 'Trygg-Hansa',
            'trygg': 'Trygg-Hansa',
            'moderna': 'Moderna Försäkringar',
            'ica': 'ICA Försäkring',
            'hedvig': 'Hedvig'
        }
        self.payoutEntity = get_payoutEntity()
        self.matchingColsPay = ['extractReference','extractOtherNumber','extractDamageNumber']
        self.matchingColsErrand = ['isReference','damageNumber','invoiceReference','ocrNumber']
        self.baseUrl = 'https://admin.direktregleringsportalen.se/errands/'         
        self.paymentQuery = self.queries['payment'].iloc[0] 
        self.errandPayQuery = self.queries['errandPay'].iloc[0] 
        self.errandLinkQuery = self.queries['errandLink'].iloc[0]
        self.payoutQuery = self.queries['payout'].iloc[0]
        
        # Pre-compile regex patterns
        self.refReg = reg.compile(r'\d+')
        self._precompiled_patterns = {}
        self._compile_info_patterns()
        
        # Cache data loaded once
        self._errand_data = None
        self._payout_data = None
        
    def _compile_info_patterns(self):
        """Pre-compile all regex patterns"""
        for _, row in self.infoReg.iterrows():
            pattern = row['regex']
            item = row['item']
            try:
                compiled_pattern = reg.compile(pattern, reg.DOTALL | reg.IGNORECASE)
                self._precompiled_patterns[item] = compiled_pattern
            except Exception as e:
                print(f"Failed to compile regex pattern for {item}: {e}")
                self._precompiled_patterns[item] = None

    def _load_errand_data(self) -> pd.DataFrame:
        """Load and cache errand data once"""
        if self._errand_data is None:
            load_start = time.time()
            print(f"🔄 Loading errand data from database...")
            self._errand_data = fetchFromDB(self.errandPayQuery.format(CONDITION=""))
            db_time = time.time() - load_start
            
            if not self._errand_data.empty:
                process_start = time.time()
                self._errand_data['createdAt'] = pd.to_datetime(
                    self._errand_data['createdAt'], utc=True
                ).dt.tz_convert('Europe/Stockholm')
                self._errand_data['settlementAmount'] = self._errand_data['settlementAmount'].fillna(0).astype(float)
                process_time = time.time() - process_start
                
                total_time = time.time() - load_start
                print(f"✅ Errand data loaded: {len(self._errand_data)} records in {total_time:.2f}s (DB: {db_time:.2f}s, Processing: {process_time:.2f}s)")
            else:
                print(f"⚠️  No errand data found")
                
        return self._errand_data
    

    def _load_payout_data(self) -> pd.DataFrame:
        """Load and cache payout data once"""
        if self._payout_data is None:
            load_start = time.time()
            print(f"🔄 Loading payout data from database...")
            self._payout_data = fetchFromDB(self.payoutQuery)
            db_time = time.time() - load_start
            
            if not self._payout_data.empty:
                process_start = time.time()
                self._payout_data['reference'] = self._payout_data['reference'].astype(str)
                process_time = time.time() - process_start
                
                total_time = time.time() - load_start
                print(f"✅ Payout data loaded: {len(self._payout_data)} records in {total_time:.2f}s (DB: {db_time:.2f}s, Processing: {process_time:.2f}s)")
            else:
                print(f"⚠️  No payout data found")
                
        return self._payout_data

    def process_payment_vectorized(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Fully vectorized payment processing"""
        # Convert timestamps
        pay['createdAt'] = pd.to_datetime(pay['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        
        # Vectorized reference extraction
        ref_mask = pay['reference'].notna()
        pay.loc[ref_mask, 'extractReference'] = pay.loc[ref_mask, 'reference'].str.extract(r'(\d+)', expand=False)
        pay['extractReference'] = pay['extractReference'].replace('', None)
        
        # Initialize columns
        pay['settlementAmount'] = 0.0
        pay['status'] = ""
        
        # Initialize columns for info parsing
        for col in self.infoItemList:
            colName = col.split('_')[1]
            if colName not in pay.columns:
                pay[colName] = None
        
        # Initialize list columns efficiently
        init_columns = ['valPay', 'valErrand', 'isReference', 'insuranceCaseId', 'referenceLink']
        for col in init_columns:
            pay[col] = [[] for _ in range(len(pay))]
        
        return pay

    def parse_info_vectorized(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Info parsing - revert to original Flask logic for exact compatibility"""
        mask = pay['info'].notna()
        for idxPay, rowPay in pay[mask].iterrows():
            ic = self.bankDict.get(rowPay['bankName'], 'None') 
            mask_info = self.infoReg['item'].str.startswith(ic)
            for _, rowInfoReg in self.infoReg[mask_info].iterrows():
                col = rowInfoReg['item'].split('_')[1]  
                item = rowInfoReg['item']
                
                # Use pre-compiled pattern
                compiled_pattern = self._precompiled_patterns.get(item)
                if compiled_pattern is None:
                    continue
                    
                match = compiled_pattern.search(rowPay['info'])
                if match:
                    matched_value = match.group(1).strip()
                    if col not in rowPay or pd.isna(rowPay.get(col)):
                        pay.at[idxPay, col] = matched_value
                    else:
                        pay.at[idxPay, 'isReference'].append(matched_value)
                                
        # Clean up duplicates
        pay.loc[pay['extractDamageNumber'] == pay['extractOtherNumber'], 'extractDamageNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractOtherNumber'], 'extractOtherNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractDamageNumber'], 'extractDamageNumber'] = None
        
        return pay

    def match_payments_vectorized(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Simplified payment matching with fallback to basic approach"""
        # Load errand data once
        errand = self._load_errand_data()
        if errand.empty:
            pay['status'] = 'No matching DRs found.'
            return pay
        
        # Simple approach: use basic matching without complex indices
        for pay_idx, pay_row in pay.iterrows():
            # Get all payment values to match
            lookup_values = []
            for col in self.matchingColsPay:
                if col in pay_row and pd.notna(pay_row[col]):
                    lookup_values.append(str(pay_row[col]))
            
            if lookup_values:
                # Find matches in errand data directly
                matches_found = []
                for val in lookup_values:
                    for errand_col in self.matchingColsErrand:
                        if errand_col in errand.columns:
                            # Vectorized matching for this column
                            col_matches = errand[
                                (errand[errand_col].notna()) &
                                (errand[errand_col].astype(str) == val) &
                                (errand['createdAt'] <= pay_row['createdAt'])
                            ]
                            if not col_matches.empty:
                                matches_found.append(col_matches)
                
                if matches_found:
                    # Combine all matches
                    all_matches = pd.concat(matches_found).drop_duplicates()
                    
                    # Check amount matches
                    amount_matches = all_matches[all_matches['settlementAmount'] == pay_row['amount']]
                    
                    if not amount_matches.empty:
                        # Perfect matches found
                        case_ids = amount_matches['insuranceCaseId'].unique().tolist()
                        refs = amount_matches['isReference'].unique().tolist()
                        
                        pay.at[pay_idx, 'insuranceCaseId'] = case_ids
                        pay.at[pay_idx, 'isReference'] = refs
                        pay.at[pay_idx, 'valPay'] = lookup_values[:len(refs)]
                        pay.at[pay_idx, 'valErrand'] = refs
                        
                        if len(case_ids) == 1:
                            pay.at[pay_idx, 'status'] = f"One DR matched perfectly."
                        else:
                            pay.at[pay_idx, 'status'] = f"Found {len(case_ids)} matching DRs."
                    else:
                        # Reference matches but not amount
                        refs = all_matches['isReference'].unique().tolist()
                        pay.at[pay_idx, 'isReference'] = refs
                        pay.at[pay_idx, 'valPay'] = lookup_values[:len(refs)]
                        pay.at[pay_idx, 'status'] = f"Found {len(refs)} relevant DR(s), but amount does not match."
                else:
                    pay.at[pay_idx, 'status'] = 'No Found'
            else:
                pay.at[pay_idx, 'status'] = 'No Found'
        
        return pay

    def match_payout_vectorized(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Vectorized payout matching"""
        payout = self._load_payout_data()
        if payout.empty:
            unmatched_mask = pay['status'].isin(['No Found', ''])
            pay.loc[unmatched_mask, 'status'] = 'No matching DRs found.'
            return pay
        
        # Process only unmatched payments
        unmatched_mask = pay['status'].isin(['No Found', ''])
        unmatched_pay = pay.loc[unmatched_mask]
        
        if unmatched_pay.empty:
            return pay
        
        step6_matches = []
        
        # Create lookup for all payment values
        for pay_idx in unmatched_pay.index:
            pay_row = pay.loc[pay_idx]
            pay_values = []
            
            for col in self.matchingColsPay:
                if col in pay_row and pd.notna(pay_row[col]):
                    pay_values.append(str(pay_row[col]))
            
            if pay_values:
                # Vectorized payout matching
                payout_matches = payout[
                    (payout['reference'].isin(pay_values)) &
                    (payout['amount'] == pay_row['amount'])
                ]
                
                if not payout_matches.empty:
                    trans_ids = payout_matches['transactionId'].dropna().astype(int).unique()
                    clinic_names = payout_matches['clinicName'].dropna().unique()
                    types = payout_matches['type'].dropna().unique()
                    
                    if len(trans_ids) == 1:
                        pay.at[pay_idx, 'status'] = f"Payment has been paid out<br>TransactionId: {trans_ids[0]}<br>Amount: {pay_row['amount'] / 100:.2f} kr<br>Clinic: {clinic_names[0] if len(clinic_names) > 0 else 'Unknown'}"
                        step6_matches.append(pay_row['id'])
                    else:
                        pay.at[pay_idx, 'status'] = f"Payment has been paid out {len(trans_ids)} times"
                        step6_matches.append(pay_row['id'])
                else:
                    pay.at[pay_idx, 'status'] = 'No matching DRs found.'
            else:
                pay.at[pay_idx, 'status'] = 'No matching DRs found.'
        
        if step6_matches:
            print(f"🎯 Step 6 matched {len(step6_matches)} payments: {step6_matches}")
        
        return pay

    def main_optimized(self, payDf: pd.DataFrame) -> pd.DataFrame:
        """Main processing pipeline - simplified to original working version"""
        start_time = time.time()
        total_payments = len(payDf)
        
        print(f"🚀 Starting payment processing for {total_payments} payments...")
        
        # Step 1: Process payments
        step1_start = time.time()
        pay = self.process_payment_vectorized(payDf.copy())
        step1_time = time.time() - step1_start
        print(f"✅ Step 1 (Process payments): {step1_time:.2f}s")
        
        # Step 2: Parse info
        step2_start = time.time()
        pay = self.parse_info_vectorized(pay)
        step2_time = time.time() - step2_start
        print(f"✅ Step 2 (Parse info): {step2_time:.2f}s")
        
        # Step 3: Match payments (using simplified vectorized approach)
        step3_start = time.time()
        pay = self.match_payments_vectorized(pay)
        step3_time = time.time() - step3_start
        print(f"✅ Step 3 (Match payments): {step3_time:.2f}s")
        
        # Step 4: Match payouts for unmatched
        step4_start = time.time()
        pay = self.match_payout_vectorized(pay)
        step4_time = time.time() - step4_start
        print(f"✅ Step 4 (Match payouts): {step4_time:.2f}s")
        
        # Format amount for display
        format_start = time.time()
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        format_time = time.time() - format_start
        print(f"✅ Final formatting: {format_time:.2f}s")
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Calculate and print performance metrics
        payments_per_second = total_payments / total_time if total_time > 0 else 0
        print(f"\n⏱️  PERFORMANCE SUMMARY:")
        print(f"   Total processing time: {total_time:.2f} seconds")
        print(f"   Payments processed: {total_payments}")
        print(f"   Processing rate: {payments_per_second:.1f} payments/second")
        print(f"   Average time per payment: {(total_time/total_payments)*1000:.1f}ms")
        
        return pay[['id','bankName','amount','info','reference','createdAt','insuranceCaseId','status']]

    def find_matches_df(self, pay: pd.DataFrame, errand: pd.DataFrame, idxPay: Any, rowPay: pd.Series) -> pd.DataFrame:
        """Find matching errands for a payment - returns DataFrame instead of IDs"""
        # Filter errands by date first
        errand_filtered = errand[errand['createdAt'] <= rowPay['createdAt']].copy()
        if errand_filtered.empty:
            return pd.DataFrame()
        
        # Fill NaN settlement amounts
        errand_filtered['settlementAmount'] = errand_filtered['settlementAmount'].fillna(0)
        
        # Find matches across all payment and errand columns
        matches_mask = pd.Series(False, index=errand_filtered.index)
        
        for colPay in self.matchingColsPay:
            valPay = rowPay[colPay]
            if pd.notna(valPay):
                str_valPay = str(valPay)
                
                # Check all errand columns for matches
                for colErrand in self.matchingColsErrand:
                    col_mask = (errand_filtered[colErrand].notna() & 
                               (errand_filtered[colErrand].astype(str) == str_valPay))
                    matches_mask |= col_mask
        
        if matches_mask.any():
            return errand_filtered[matches_mask].copy()
        else:
            return pd.DataFrame()

    def _match_by_info_original(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Match payments by info following original Flask logic exactly"""
        if errand.empty:
            pay['status'] = 'No matching DRs found.'
            return pay
        
        step3_matches = []
        
        for idxPay, rowPay in pay.iterrows():
            matches = self.find_matches_df(pay, errand, idxPay, rowPay)
            
            if matches is not None and not matches.empty:
                # Update payment with match info
                match_info = {
                    'insuranceCaseId': list(matches['insuranceCaseId'].unique()),
                    'valPay': [],
                    'valErrand': [],
                    'isReference': []
                }
                
                # Collect reference info
                for col in self.matchingColsPay:
                    if col in rowPay and pd.notna(rowPay[col]):
                        match_info['valPay'].append(str(rowPay[col]))
                
                for col in self.matchingColsErrand:
                    if col in matches.columns:
                        vals = matches[col].dropna().astype(str).unique().tolist()
                        match_info['valErrand'].extend(vals)
                        match_info['isReference'].extend(vals)
                
                # Update payment row
                for key, value in match_info.items():
                    pay.at[idxPay, key] = value
                
                # Check if amounts match exactly
                exact_matches = matches[matches['settlementAmount'] == rowPay['amount']]
                if not exact_matches.empty:
                    status_msg = "One DR matched perfectly." if len(exact_matches) == 1 else f"Found {len(exact_matches)} matching DRs."
                    pay.at[idxPay, 'status'] = status_msg
                    step3_matches.append(rowPay['id'])
                else:
                    pay.at[idxPay, 'status'] = f"Found {len(matches)} relevant DR(s), but amount does not match."
            else:
                pay.at[idxPay, 'status'] = 'No Found'
        
        if step3_matches:
            print(f"🎯 Step 3 matched {len(step3_matches)} payments: {step3_matches}")
        
        return pay

    def _reminder_unmatched_amount_original(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Handle unmatched amounts following original Flask logic"""
        errand = self._load_errand_data()
        if errand.empty:
            return pay
        
        unmatched_mask = pay['status'].isin(["No Found", ""])
        for idxPay, rowPay in pay[unmatched_mask].iterrows():
            # Get settlement amounts for this payment amount
            amount_matches = errand[errand['settlementAmount'] == rowPay['amount']]
            if not amount_matches.empty:
                pay.at[idxPay, 'settlementAmount'] = rowPay['amount']
                for _, rowSub in amount_matches.iterrows():
                    pay.at[idxPay, 'settlementAmount'] += float(rowSub['settlementAmount'])
        
        return pay

    def _match_entity_and_amount_original(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Match by entity and amount following original Flask logic with proper insurance company filtering"""
        if errand.empty:
            return pay
        
        unmatched_mask = pay['status'].isin(["No Found", ""])
        step5_matches = []
        
        for idxPay, rowPay in pay[unmatched_mask].iterrows():
            # Get insurance company reference for this payment's bank
            ic_ref = self.bankDict.get(rowPay['bankName'], 'None')
            if ic_ref == 'None':
                continue
                
            # Get expected insurance company name for filtering
            expected_ic_name = self.refToInsuranceNameDict.get(ic_ref)
            if not expected_ic_name:
                continue
                
            # Find errands with matching insurance company, amount and date
            ic_matches = errand[
                (errand.get('insuranceCompanyName', pd.Series()).str.contains(expected_ic_name, case=False, na=False)) &
                (errand['settlementAmount'] == rowPay['amount']) &
                (errand['createdAt'] <= rowPay['createdAt'])
            ]
            
            if not ic_matches.empty:
                case_ids = ic_matches['insuranceCaseId'].unique().tolist()
                refs = ic_matches['isReference'].unique().tolist()
                
                pay.at[idxPay, 'insuranceCaseId'] = case_ids
                pay.at[idxPay, 'isReference'] = refs
                pay.at[idxPay, 'valErrand'] = refs
                
                if len(case_ids) == 1:
                    pay.at[idxPay, 'status'] = "One DR matched perfectly."
                    step5_matches.append(rowPay['id'])
                else:
                    pay.at[idxPay, 'status'] = f"Found {len(case_ids)} matching DRs."
                    step5_matches.append(rowPay['id'])
        
        if step5_matches:
            print(f"🎯 Step 5 matched {len(step5_matches)} payments: {step5_matches}")
        
        return pay

    def calculate_statistics(self, pay: pd.DataFrame) -> Dict[str, Any]:
        """Calculate matching statistics with performance info"""
        stats_start = time.time()
        
        all_count = len(pay)
        if all_count == 0:
            return {'total': 0, 'matched': 0, 'matched_rate': 0, 'perfect_matched': 0, 'perfect_rate': 0, 'paid_out': 0, 'paid_out_rate': 0, 'unmatched': 0, 'unmatched_rate': 0}
        
        # Calculate statistics
        matched = pay[~pay['status'].str.contains('No Found|No matching DRs found', na=False, regex=True)]
        perfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False)]
        payout = pay[pay['status'].str.contains('paid out', na=False)]
        unmatched = pay[pay['status'].str.contains('No Found|No matching DRs found', na=False, regex=True)]
        
        stats_time = time.time() - stats_start
        
        # Print detailed matching statistics
        print(f"\n📊 MATCHING STATISTICS:")
        print(f"   Total payments: {all_count}")
        print(f"   ✅ Matched: {len(matched)} ({len(matched) / all_count * 100:.1f}%)")
        print(f"   🎯 Perfect matches: {len(perfect)} ({len(perfect) / all_count * 100:.1f}%)")
        print(f"   💰 Paid out: {len(payout)} ({len(payout) / all_count * 100:.1f}%)")
        print(f"   ❌ Unmatched: {len(unmatched)} ({len(unmatched) / all_count * 100:.1f}%)")
        print(f"   Statistics calculation time: {stats_time:.3f}s")
        
        return {
            'total': all_count,
            'matched': len(matched),
            'matched_rate': len(matched) / all_count * 100,
            'perfect_matched': len(perfect),
            'perfect_rate': len(perfect) / all_count * 100,
            'paid_out': len(payout),
            'paid_out_rate': len(payout) / all_count * 100,
            'unmatched': len(unmatched),
            'unmatched_rate': len(unmatched) / all_count * 100
        }