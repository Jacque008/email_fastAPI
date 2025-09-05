from typing import List
import pandas as pd
from dataclasses import dataclass, field
from ..schemas.payment import PaymentIn, PaymentOut
from ..services.payment_service import PaymentService
from ..services.utils import fetchFromDB, model_to_dataframe


@dataclass
class PaymentDataset:
    """Dataset class that chains all atomic functions for payment matching - optimized"""
    service: PaymentService = field(default_factory=PaymentService)

    def match_payments(self, payments: List[PaymentIn]) -> List[PaymentOut]:
        """
        Optimized payment matching using vectorized operations
        
        Args:
            payments: List of PaymentIn objects to process
            
        Returns:
            List of PaymentOut objects with matching results
        """
        if not payments:
            return []
        
        # Convert PaymentIn objects to DataFrame
        pay_df = model_to_dataframe(payments)
        
        # Chain the original atomic functions following Flask logic
        result_df = self._chain_payment_processing(pay_df)
        
        # Convert back to PaymentOut objects
        return self._dataframe_to_payments(result_df)
    
    def get_matching_statistics(self, payments: List[PaymentIn]) -> dict:
        """
        Get matching statistics for a set of payments - optimized
        
        Args:
            payments: List of PaymentIn objects to analyze
            
        Returns:
            Dictionary with matching statistics
        """
        if not payments:
            return {}
            
        # Process payments once and get results
        pay_df = self._payments_to_dataframe(payments)
        result_df = self._chain_payment_processing(pay_df)
        
        return self.service.calculate_statistics(result_df)
    
    # def _payments_to_dataframe(self, payments: List[PaymentIn]) -> pd.DataFrame:
    #     """Convert list of PaymentIn objects to pandas DataFrame"""
    #     payment_dicts = []
    #     for payment in payments:
    #         payment_dict = payment.model_dump()
    #         payment_dicts.append(payment_dict)
        
    #     return pd.DataFrame(payment_dicts)
    
    def _chain_payment_processing(self, pay_df: pd.DataFrame) -> pd.DataFrame:
        """Chain all atomic functions following original Flask logic with performance optimizations"""
        import time
        start_time = time.time()
        
        print(f"🚀 Starting payment processing for {len(pay_df)} payments...")
        
        # Pre-load all data once for better performance
        print("📊 Pre-loading database data...")
        db_start = time.time()
        errand = fetchFromDB(self.service.errand_pay_query.format(CONDITION="AND 1=1"))

        print("========74806******\n", errand.loc[errand['errandId']==74806])
        payout = fetchFromDB(self.service.payout_query)
        db_time = time.time() - db_start
        print(f"✅ Database loaded in {db_time:.2f}s")
        
        # Pre-process database data
        if not errand.empty:
            errand['createdAt'] = pd.to_datetime(errand['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
            errand['settlementAmount'] = errand['settlementAmount'].fillna(0).astype(float)
        if not payout.empty:
            payout['reference'] = payout['reference'].astype(str)
        
        # Step 1: Process payments (vectorized)
        step1_start = time.time()
        pay = self.service.process_payment(pay_df.copy())
        print(f"✅ Step 1 (Process): {time.time() - step1_start:.2f}s")
        
        # Step 2: Parse info (keep original logic but add timing)
        step2_start = time.time()
        pay = self.service.parse_info(pay)
        print(f"✅ Step 2 (Parse info): {time.time() - step2_start:.2f}s")
        
        # Step 3: Match by info (optimized but same logic)
        if not errand.empty:
            step3_start = time.time()
            pay = self._optimized_match_by_info(pay, errand)
            print(f"✅ Step 3 (Match by info - optimized): {time.time() - step3_start:.2f}s")
            
            # Step 4: Handle unmatched amounts (RESTORED)
            step4_start = time.time()
            pay = self.service.reminder_unmatched_amount(pay)
            print(f"✅ Step 4 (Reminder unmatched): {time.time() - step4_start:.2f}s")
            
            # Step 5: Match by entity and amount (RESTORED)
            step5_start = time.time()
            pay = self.service.match_entity_and_amount(pay, errand)
            print(f"✅ Step 5 (Match entity/amount): {time.time() - step5_start:.2f}s")
        
        # Step 6: Match payouts
        if not payout.empty:
            step6_start = time.time()
            pay = self.service.match_payout(pay, payout)
            print(f"✅ Step 6 (Match payouts): {time.time() - step6_start:.2f}s")
        
        # Format amount for display (vectorized)
        format_start = time.time()
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        print(f"✅ Format amounts: {time.time() - format_start:.2f}s")
        
        total_time = time.time() - start_time
        print(f"\n⏱️ Total processing time: {total_time:.2f}s ({len(pay_df)/total_time:.1f} payments/sec)")
        
        return pay
    
    def _optimized_match_by_info(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Optimized version of match_by_info that reduces loops but keeps same logic"""
        mask = (pay['info'].notna() | pay['extractReference'].notna())
        
        # Pre-filter errands and create lookup indices for better performance
        errand_lookup = {}
        for col in self.service.matching_cols_errand:
            if col in errand.columns:
                # Create lookup dict for each errand column
                valid_errands = errand[errand[col].notna()].copy()
                if not valid_errands.empty:
                    valid_errands[col] = valid_errands[col].astype(str)
                    errand_lookup[col] = valid_errands.groupby(col).apply(lambda x: x.index.tolist()).to_dict()
        
        # Process each payment (keep same logic but with pre-computed lookups)
        for idxPay, rowPay in pay[mask].iterrows():
            matchedInsuranceCaseID = self._fast_find_matches(pay, errand, errand_lookup, idxPay, rowPay)
            qty = len(matchedInsuranceCaseID)
            if qty > 0:
                pay.at[idxPay, 'insuranceCaseId'].extend(matchedInsuranceCaseID) 
                links = self._safe_generate_links(rowPay['insuranceCaseId'], rowPay['valErrand'], 'ic.id')
                pay.at[idxPay, 'referenceLink'] = links
                if qty == 1:
                    pay.at[idxPay, 'status'] = f"One DR matched perfectly (reference: {', '.join(links)})."
                elif qty > 1:
                    pay.at[idxPay, 'status'] = f"Found {qty} matching DRs (references: {', '.join(links)}) and the payment amount matches each one."
            else:
                pay.at[idxPay, 'status'] = 'No Found'
        
        return pay
    
    def _fast_find_matches(self, pay: pd.DataFrame, errand: pd.DataFrame, errand_lookup: dict, idxPay, rowPay: pd.Series) -> List[int]:
        """Faster version of find_matches using pre-computed lookups"""
        matched = {colPay: [] for colPay in self.service.matching_cols_pay}
        
        # Filter errands by date first (vectorized)
        date_mask = errand['createdAt'] <= rowPay['createdAt']
        if not date_mask.any():
            return []
        
        for colPay in self.service.matching_cols_pay:
            valPay = rowPay[colPay]
            if pd.notna(valPay):
                valAmount = rowPay['amount']
                str_valPay = str(valPay)
                
                # Use pre-computed lookup instead of scanning all errands
                matched_indices = set()
                for colErrand in self.service.matching_cols_errand:
                    if colErrand in errand_lookup and str_valPay in errand_lookup[colErrand]:
                        indices = errand_lookup[colErrand][str_valPay]
                        # Filter by date mask
                        date_filtered_indices = [idx for idx in indices if date_mask.loc[idx]]
                        matched_indices.update(date_filtered_indices)
                
                if matched_indices:
                    matched_errands = errand.loc[list(matched_indices)]
                    
                    # Update pay DataFrame with matched references (same logic)
                    current_refs = set(str(ref) for ref in pay.at[idxPay, 'isReference'])
                    new_refs = matched_errands['isReference'].astype(str).tolist()
                    for ref in new_refs:
                        if ref not in current_refs:
                            pay.at[idxPay, 'isReference'].append(ref)
                            pay.at[idxPay, 'valPay'].append(str_valPay)
                            current_refs.add(ref)
                    
                    # Find amount matches (same logic)
                    amount_mask = (matched_errands['settlementAmount'].fillna(0) == valAmount)
                    if amount_mask.any():
                        matched_case_ids = matched_errands[amount_mask]['insuranceCaseId'].astype(int).tolist()
                        matched[colPay].extend([cid for cid in matched_case_ids if cid not in matched[colPay]])
                        
                        # Update valErrand list (same logic)
                        matched_vals = matched_errands[amount_mask][self.service.matching_cols_errand].values.flatten()
                        for val in matched_vals:
                            if pd.notna(val):
                                pay.at[idxPay, 'valErrand'].append(str(val))
        
        # Same intersection/union logic as original
        matchedLists = [set(matched[colPay]) for colPay in self.service.matching_cols_pay if matched[colPay]]
        if matchedLists:
            matchedInsuranceCaseID = list(set.intersection(*matchedLists)) if len(matchedLists) > 1 else list(matchedLists[0])
            if not matchedInsuranceCaseID:
                matchedInsuranceCaseID = list(set.union(*matchedLists))
        else:
            matchedInsuranceCaseID = []
        
        return matchedInsuranceCaseID
    
    def _safe_generate_links(self, colList1: List[int], colList2: List[str], condition: str) -> List[str]:
        """Fixed version of generate_links with proper SQL syntax"""
        links = []
        if len(colList1) == 0:
            return links
            
        # Create proper WHERE condition without leading AND
        if condition == 'ic.reference':
            ids_str = ', '.join([f"'{id}'" for id in colList1])
            condition_sql = f"{condition} IN ({ids_str})"
        elif condition == 'ic.id':
            ids_str = ', '.join([str(id) for id in colList1])
            condition_sql = f"{condition} IN ({ids_str})"
        else:
            return links
            
        # Single batch query with proper WHERE condition
        try:
            result = fetchFromDB(self.service.errand_link_query.format(CONDITION=condition_sql))
            if not result.empty:
                for _, row in result.iterrows():
                    # Use the correct column names from the actual query
                    link = f'<a href="{self.service.base_url}{row["errandNumber"]}" target="_blank">{row["reference"]}</a>'
                    links.append(link)
            else:
                print("DEBUG: Query returned empty result")
        except Exception as e:
            # Fallback: return simple text links without database lookup
            for ref in colList2:
                links.append(f'<span>Reference: {ref}</span>')
                
        return links
    
    def _dataframe_to_payments(self, df: pd.DataFrame) -> List[PaymentOut]:
        """Convert pandas DataFrame to list of PaymentOut objects"""
        payments = []
        
        for _, row in df.iterrows():
            payment = PaymentOut(
                id=row['id'],
                reference=row.get('reference'),
                bankName=row['bankName'],
                amount=row['amount'],  # Already formatted as "X.XX kr"
                info=row.get('info'),
                createdAt=row['createdAt'],
                insuranceCaseId=row.get('insuranceCaseId', []),
                status=row.get('status', ''),
                valPay=row.get('valPay', []),
                valErrand=row.get('valErrand', []),
                isReference=row.get('isReference', []),
                referenceLink=row.get('referenceLink', []),
                settlementAmount=row.get('settlementAmount', 0.0),
                extractReference=row.get('extractReference'),
                extractDamageNumber=row.get('extractDamageNumber'),
                extractOtherNumber=row.get('extractOtherNumber')
            )
            payments.append(payment)
            
        return payments