import regex as reg
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from itertools import combinations
from .base_service import BaseService
from .utils import get_payoutEntity, fetchFromDB, tz_convert


class PaymentService(BaseService):
    """Service for payment matching functionality"""
    
    def __init__(self):
        super().__init__()
        self.info_reg = pd.read_csv(f"{self.folder}/infoReg.csv")
        self.info_item_list = self.info_reg.item.to_list()
        self.bank_map = pd.read_csv(f"{self.folder}/bankMap.csv")
        self.bank_dict = self.bank_map.set_index('bankName')['insuranceCompanyReference'].to_dict()
        self.payout_entity = get_payoutEntity()
        self.matching_cols_pay = ['extractReference','extractOtherNumber','extractDamageNumber']
        self.matching_cols_errand = ['isReference','damageNumber','invoiceReference','ocrNumber']
        self.base_url = 'https://admin.direktregleringsportalen.se/errands/'         
        # self.payment_query = self.queries['payment'].iloc[0] 
        self.errand_pay_query = self.queries['errandPay'].iloc[0] 
        self.errand_link_query = self.queries['errandLink'].iloc[0]
        
        self.payout_query = self.queries['payout'].iloc[0]
        
        # Pre-compile regex patterns for better performance
        self.ref_reg = reg.compile(r'\d+')
        self._precompiled_patterns = {}
        self._compile_info_patterns()
        
        # Cache expensive dictionary operations
        self._entity_dicts_cached = False
        self._payout_entity_source = {}
        self._ic_dict = {}
        self._clinic_dict = {}
        
    def _compile_info_patterns(self):
        """Pre-compile all regex patterns from infoReg for better performance"""
        for _, row in self.info_reg.iterrows():
            pattern = row['regex']
            item = row['item']
            try:
                compiled_pattern = reg.compile(pattern, reg.DOTALL | reg.IGNORECASE)
                self._precompiled_patterns[item] = compiled_pattern
            except Exception as e:
                print(f"Failed to compile regex pattern for {item}: {e}")
                self._precompiled_patterns[item] = None
                
    def _get_entity_dicts(self):
        """Cache entity dictionaries to avoid repeated computation"""
        if not self._entity_dicts_cached:
            self._payout_entity_source = self.payout_entity.set_index('payoutEntity')['source'].to_dict()
            self._ic_dict = self.payout_entity.loc[self.payout_entity['source'] == 'Insurance_Company'].groupby('payoutEntity')['clinic'].apply(list).to_dict()
            self._clinic_dict = self.payout_entity.loc[self.payout_entity['source'] == 'Clinic'].groupby('payoutEntity')['clinic'].apply(list).to_dict()
            self._entity_dicts_cached = True
        return self._payout_entity_source, self._ic_dict, self._clinic_dict

    def load_preprocess_database(self, ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        errand = fetchFromDB(self.errand_pay_query.format(CONDITION=""))
        errand = tz_convert(errand, 'createdAt')
        errand['settlementAmount'] = errand['settlementAmount'].fillna(0).astype(float)
   
        payout = fetchFromDB(self.payout_query)
        if not payout.empty:
            payout['reference'] = payout['reference'].astype(str)
        
        return errand, payout
    
    def _build_errand_lookup(self, errand, row_pay) -> Dict[str, Dict[str, List[int]]]:
        date_mask = (errand['createdAt'] <= row_pay['createdAt'])
        if not date_mask.any():
            return {}
        
        errand = errand.loc[date_mask].copy()
        errand_lookup = {}
        for col in self.matching_cols_errand:
            if col in errand.columns:
                s = errand[col]
                notna_mask = s.notna()
                if notna_mask.any():
                    filtered_s = s[notna_mask]
                    groups = filtered_s.groupby(filtered_s).indices  
                    errand_lookup[col] = {val: filtered_s.index.take(pos_arr).tolist()
                        for val, pos_arr in groups.items()}
        return errand_lookup
                    
    def init_payment(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Process payment data - extract references and initialize columns"""
        # pay['createdAt'] = pd.to_datetime(pay['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm') 
        pay = tz_convert(pay, 'createdAt')
        mask = pay['reference'].notna()
        if mask.any():
            pay.loc[mask,'extractReference'] = pay.loc[mask,'reference'].apply(lambda x: ''.join(self.ref_reg.findall(x)) if isinstance(x, str) else None)
        pay.loc[pay['extractReference'].notna(),'extractReference'] = pay.loc[pay['extractReference'].notna(),'extractReference'].replace('', None)
        pay['settlementAmount'] = 0
        pay['status'] = ""

        for col in self.info_item_list:
            colName = col.split('_')[1]
            if colName not in pay.columns:
                pay[colName] = None
        
        init_columns = ['val_pay', 'val_errand', 'isReference', 'insuranceCaseId', 'referenceLink']
        for col in init_columns:
            pay[col] = [[] for _ in range(len(pay))]
        
        return pay[['id','val_pay','val_errand','amount','settlementAmount','isReference','insuranceCaseId','referenceLink',
                    'status','extractReference','extractDamageNumber','extractOtherNumber','bankName','info','reference','createdAt']]

    def parse_info(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Parse payment info field using regex patterns - optimized with pre-compiled patterns"""
        mask = pay['info'].notna()
        for idx, row_pay in pay.loc[mask].iterrows():
            fb = self.bank_dict.get(row_pay['bankName'], 'None') 
            mask_info = self.info_reg['item'].str.startswith(fb)
            for _, rowInfoReg in self.info_reg[mask_info].iterrows():
                col = rowInfoReg['item'].split('_')[1]  
                item = rowInfoReg['item']
                compiled_pattern = self._precompiled_patterns.get(item)
                if compiled_pattern is None:
                    continue
                    
                match = compiled_pattern.search(row_pay['info'])
                if match:
                    matched_value = match.group(1).strip()
                    if col not in row_pay or pd.isna(row_pay.get(col)):
                        pay.at[idx, col] = matched_value
                    else:
                        pay.at[idx, 'isReference'].append(matched_value)
                                
        # Clean up duplicates
        pay.loc[pay['extractDamageNumber'] == pay['extractOtherNumber'], 'extractDamageNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractOtherNumber'], 'extractOtherNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractDamageNumber'], 'extractDamageNumber'] = None

        return pay

    def match_by_info(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        mask = (pay['info'].notna() | pay['extractReference'].notna())
        
        for idx, row_pay in pay.loc[mask].iterrows():
            matched_ic_ids = self._find_matches(pay, errand, idx, row_pay)
            qty = len(matched_ic_ids)
            if qty > 0:
                pay.at[idx, 'insuranceCaseId'].extend(matched_ic_ids) 
                links = self._generate_links(row_pay['insuranceCaseId'], row_pay['val_errand'], 'ic.id')
                pay.at[idx, 'referenceLink'] = links
                if qty == 1:
                    pay.at[idx, 'status'] = f"One DR matched perfectly (reference: {', '.join(links)})."
                else:
                    pay.at[idx, 'status'] = (f"Found {qty} matching DRs (references: {', '.join(links)}) "
                                             f"and the payment amount matches each one.")
            else:
                pay.at[idx, 'status'] = 'No Found'
        
        return pay
    
    def _find_matches(self, pay: pd.DataFrame, errand: pd.DataFrame, idx, row_pay: pd.Series) -> List[int]:
        matched = {col_pay: [] for col_pay in self.matching_cols_pay}
        date_mask = (errand['createdAt'] <= row_pay['createdAt'])
        if not date_mask.any():
            return []

        val_amount = row_pay['amount']
        errand_lookup = self._build_errand_lookup(errand, row_pay)
        for col_pay in self.matching_cols_pay:
            val_pay = row_pay[col_pay]
            if pd.isna(val_pay):
                continue

            for col_errand in self.matching_cols_errand:
                indices = errand_lookup.get(col_errand, {}).get(val_pay, [])
                if not indices:
                    continue
                
                idxs_sorted = sorted(indices)
                matched_rows = errand.loc[idxs_sorted]

                current_refs = set(pay.at[idx, 'isReference'])
                for ref in matched_rows['isReference']:
                    if ref not in current_refs:
                        pay.at[idx, 'isReference'].append(ref)
                        pay.at[idx, 'val_pay'].append(val_pay)
                        current_refs.add(ref)

                rows_amount_ok = matched_rows[matched_rows['settlementAmount'] == val_amount]
                if not rows_amount_ok.empty:
                    for _, r in rows_amount_ok.iterrows():
                        icid = int(r['insuranceCaseId'])
                        if icid not in matched[col_pay]:
                            matched[col_pay].append(icid)
                        pay.at[idx, 'val_errand'].append(r[col_errand])

        matchedLists = [matched[c] for c in self.matching_cols_pay if matched[c]]
        if not matchedLists:
            return []

        first = matchedLists[0]
        inter = [x for x in first if all(x in s for s in matchedLists[1:])]
        if inter:
            return inter

        union = []
        seen = set()
        for s in matchedLists:
            for x in s:
                if x not in seen:
                    seen.add(x)
                    union.append(x)
        return union
    
    def _compute_match(self, matched: Dict[str, List[Any]]) -> List[Any]:
        matched_lists: List[List[Any]] = []
        for c in self.matching_cols_pay:
            lst = matched.get(c)
            if isinstance(lst, list) and len(lst) > 0:
                matched_lists.append(lst)

        if not matched_lists:
            return []

        first = matched_lists[0]
        inter = [x for x in first if all(x in s for s in matched_lists[1:])]
        if inter:
            return inter

        seen = set()
        union: List[Any] = []
        for s in matched_lists:
            for x in s:
                if x not in seen:
                    seen.add(x)
                    union.append(x)
        return union

    def _generate_links(self, ic_ids: List[Any], vals_errand: List[Any], condition: str) -> List[str]:
        links = []
        if not ic_ids:
            return links
        
        for id, valErrand in zip(ic_ids, vals_errand):
            if condition == 'ic.reference':
                cond = f"{condition} = '{id}'"
            elif condition == 'ic.id':
                cond = f"{condition} = {id}"
            result = fetchFromDB(self.errand_link_query.format(CONDITION=cond))
            if not result.empty: 
                errandNumber = result.iloc[0]['errandNumber']
                ref = result.iloc[0]['reference']
                link = f'<a href="{self.base_url}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by {valErrand}">{ref}</a>'
                links.append(link)
            else:
                links.append(f'{ref} (No Corresponding Link)')
        return links

    def reminder_unmatched_amount(self, pay: pd.DataFrame) -> pd.DataFrame:
        mask = (pay['status'].isin(["No Found",""]))
        for idx, row_pay in pay.loc[mask].iterrows():
            matched_ic_ids, isReference, links = [], [], []
            ref_amount_dict = {}
            msg = "No Found"
            
            # Collect all possible references
            if (len(row_pay['isReference']) > 0):
                for ref in row_pay['isReference']:
                    if ref not in isReference:
                        isReference.append(ref)
            
            if pd.notna(row_pay['extractReference']) and (len(row_pay['extractReference']) == 10) and (row_pay['extractReference'] not in isReference):
                isReference.append(str(row_pay['extractReference']))                 
            if pd.notna(row_pay['extractOtherNumber']) and (len(row_pay['extractOtherNumber']) == 10) and (row_pay['extractOtherNumber'] not in isReference):
                isReference.append(str(row_pay['extractOtherNumber']))
            if pd.notna(row_pay['extractDamageNumber']) and (len(row_pay['extractDamageNumber']) == 10) and (row_pay['extractDamageNumber'] not in isReference):
                isReference.append(str(row_pay['extractDamageNumber']))
                
            if len(isReference) > 0:
                condition_sql = f"""AND ic.reference IN ({', '.join([f"'{ref}'" for ref in set(isReference)])})"""  
                sub_errand = fetchFromDB(self.errand_pay_query.format(CONDITION=condition_sql))

                sub_errand.loc[sub_errand['settlementAmount'].isna(), 'settlementAmount'] = 0
                if not sub_errand.empty:
                    for _, row_sub in sub_errand.iterrows():
                        if row_sub['insuranceCaseId'] not in matched_ic_ids:
                            settlement_amount = row_sub['settlementAmount'] if pd.notna(row_sub['settlementAmount']) else 0
                            pay.at[idx, 'settlementAmount'] += float(settlement_amount)
                            matched_ic_ids.append(row_sub['insuranceCaseId'])
                            
                        if str(row_sub['isReference']) not in [str(ref) for ref in pay.at[idx, 'isReference']]:
                            pay.at[idx, 'isReference'].append(str(row_sub['isReference']))
                            pay.at[idx, 'val_pay'].append(str(row_sub['isReference']))
                            
                        if str(row_sub['isReference']) not in ref_amount_dict:
                            settlement_amount = row_sub['settlementAmount'] if pd.notna(row_sub['settlementAmount']) else 0
                            ref_amount_dict[str(row_sub['isReference'])] = float(settlement_amount)

            ref_list = pay.at[idx, 'isReference']
            val_errand_list = pay.at[idx, 'val_pay']
            links = self._generate_links(ref_list, val_errand_list, 'ic.reference')
            pay.at[idx, 'referenceLink'] = links
            
            qty = len(matched_ic_ids)
            if qty > 0:
                total_settlement_amount = pay.at[idx, 'settlementAmount']
                row_payment_amount = row_pay['amount']

                if row_payment_amount == total_settlement_amount:
                    if qty == 1:
                        msg = f"One DR matched perfectly (reference: {', '.join(links)})."
                    else:       
                        msg = (f"Found {qty} matching DRs (references: {', '.join(links)}), "
                               f"and the total amount matches the payment.")
                else:
                    matched_references = self._partly_amount_matching(ref_amount_dict, row_payment_amount)
                    if matched_references:
                        matched_links = [link for link in links if any(ref in link for ref in matched_references)]
                        if len(matched_references) == 1:
                            msg = f"One DR matched perfectly (reference: {', '.join(links)})."
                        else:
                            msg = f"Found {len(matched_references)} matching DRs (references: {', '.join(matched_links)}), and the total amount matches the payment."
                    else:
                        if qty == 1:
                            msg = f"Found 1 relevant DR (reference: {', '.join(links)}), but the amount does not match."
                        else:
                            msg = f"Found {qty} relevant DRs (references: {', '.join(links)}), but the amounts do not match."

            pay.at[idx, 'status'] = msg

        return pay   

    def _partly_amount_matching(self, ref_amount_dict: Dict[str, float], target_amount: float) -> Optional[List[str]]:
        """Find combinations of references that match the target amount"""
        references = list(ref_amount_dict.keys())
        amounts = list(ref_amount_dict.values())
        
        for r in range(1, len(amounts) + 1):
            for combo in combinations(zip(references, amounts), r):
                combo_references, combo_amounts = zip(*combo)
                if sum(combo_amounts) == target_amount:
                    return list(combo_references)
                
        return None
 
    def _msg_for_one(self, one_line_df: pd.DataFrame, source: str, amount: float) -> str:
        errand_number = one_line_df.iloc[0]['errandNumber']
        ref = one_line_df.iloc[0]['isReference']
        settlement_amount = one_line_df.iloc[0]['settlementAmount'] if pd.notna(one_line_df.iloc[0]['settlementAmount']) else 0
        
        if source == 'Insurance_Company':
            entity = one_line_df.iloc[0]['insuranceCompanyName'] 
        elif source == 'Clinic':
            entity = one_line_df.iloc[0]['clinicName']
            
        link = f'<a href="{self.base_url}{errand_number}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>'
        
        if amount == float(settlement_amount):
            msg = f"One DR matched perfectly (reference: {link}) by both entity and amount."
        else:
            msg = "No Found"
            
        return msg

    def match_entity_and_amount(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        payout_entity_source, fb_dict, clinic_dict = self._get_entity_dicts()

        errand = errand.copy()
        errand["fb_lower"] = errand["insuranceCompanyName"].str.lower()

        errand_date = errand["createdAt"]
        fb_lower = errand["insuranceCompanyName"].str.lower()
        clinic_lower = errand["clinicName"].str.lower()

        mask = pay['status'].isin(["No Found", ""])

        for idx, rowPay in pay.loc[mask].iterrows():
            amount = rowPay['amount']
            source = payout_entity_source.get(rowPay['bankName'], "Unknown")
            msg = "No Found"
            if source == 'Insurance_Company':
                fb_list = fb_dict.get(rowPay['bankName'], [])
                if not fb_list:
                    pay.at[idx, 'status'] = "No Found"
                    continue

                fb_lower_list = [x.lower() for x in fb_list if isinstance(x, str)]
                entity_matched = errand.loc[(errand_date <= rowPay['createdAt']) & (fb_lower.isin(fb_lower_list))]

            elif source == 'Clinic':
                clinic_list = clinic_dict.get(rowPay['bankName'], [])
                if not clinic_list:
                    pay.at[idx, 'status'] = "No Found"
                    continue
                clinic_lower_list = [x.lower() for x in clinic_list if isinstance(x, str)]
                entity_matched = errand.loc[(errand_date <= rowPay['createdAt']) & (clinic_lower.isin(clinic_lower_list))]
            else:
                pay.at[idx, 'status'] = "No Found"
                continue

            qty = len(entity_matched)
            if qty == 1:
                msg = self._msg_for_one(entity_matched, source, amount)

            elif qty > 1:
                for _, group_df in entity_matched.groupby(['insuranceCompanyName', 'clinicName', 'animalId']):
                    if len(group_df) == 1:
                        temp = self._msg_for_one(group_df, source, amount)
                        msg = temp
                    else:
                        ref_amount_dict, links = {}, []
                        for _, row in group_df.iterrows():
                            sa = row['settlementAmount']
                            if pd.notna(sa):
                                ref_amount_dict[row['isReference']] = sa  # 不把 NaN 当 0
                            errand_number = row['errandNumber']
                            ref = row['isReference']
                            entity = row['insuranceCompanyName'] if source == 'Insurance_Company' else row['clinicName']
                            link = (f'<a href="{self.base_url}{errand_number}" target="_blank" '
                                    f'style="background-color: gray; color: white; padding: 2px 5px;" '
                                    f'title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>')
                            links.append((ref, link))

                        matched_refs = self._partly_amount_matching(ref_amount_dict, amount)
                        if matched_refs:
                            matched_links = [link for ref, link in links if ref in matched_refs]
                            if len(matched_refs) == 1:
                                msg = (f"One DR matched perfectly (reference: {', '.join(matched_links)}) "
                                       f"by both entity and amount.")
                            else:
                                msg = (f"Found {len(matched_refs)} matching DRs "
                                       f"(references: {', '.join(matched_links)}) by entity, "
                                       f"and the total amount matches the payment.")
                        else:
                            msg = "No Found"
                    
            else:
                msg = "No Found"

            pay.at[idx, 'status'] = msg

        return pay
 
    def match_payout(self, pay: pd.DataFrame, payout: pd.DataFrame) -> pd.DataFrame:
        """Match against payout records - optimized with vectorized operations"""
        if payout.empty:
            mask = (pay['status'].isin(["No Found",""]))
            pay.loc[mask, 'status'] = 'No matching DRs found.'
            return pay
            
        mask = pay['status'].isin(["No Found", ""])
        ref_series = payout['reference']
        amt_series = payout['amount']
        
        for idx, row_pay in pay.loc[mask].iterrows():
            matched_trans_ids, matched_clinic_name, matched_type = [], [], []
            amount_eq = (amt_series == row_pay['amount'])

            for col in self.matching_cols_pay:
                val_pay = row_pay[col]
                if pd.isna(val_pay):
                    continue
                hits = payout[(ref_series == val_pay) & amount_eq]

                for _, hit in hits.iterrows():
                    tid = hit['transactionId']
                    if pd.notna(tid):
                        tid = int(tid)
                        if tid not in matched_trans_ids:
                            matched_trans_ids.append(tid)

                    cname = hit['clinicName']
                    if pd.notna(cname) and cname not in matched_clinic_name:
                        matched_clinic_name.append(cname)

                    typ = hit['type']
                    if pd.notna(typ) and typ not in matched_type:  
                        matched_type.append(typ)

            qty = len(matched_trans_ids)
            if qty == 1:
                pay.at[idx, 'status'] = (f"Payment has been paid out<br>"
                                         f"             TransactionId: {list(matched_trans_ids)[0]}<br>"
                                         f"             Amount: {row_pay['amount'] / 100:.2f} kr<br>"
                                         f"             Clinic Name: {list(matched_clinic_name)[0]}<br>"
                                         f"             Type: {list(matched_type)[0] if matched_type else ''}")
            elif qty > 0:
                pay.at[idx, 'status'] = (f"Payment has been paid out {qty} times<br>"
                                         f"    TransactionId:{' '.join(map(str, sorted(matched_trans_ids)))}<br>"
                                         f"           Amount: {row_pay['amount'] / 100:.2f} kr<br>"
                                         f"      Clinic Name: {' '.join(sorted(matched_clinic_name))}<br>"
                                         f"             Type: {' '.join(sorted(matched_type))}")
            else:
                pay.at[idx, 'status'] = 'No matching DRs found.'   

        return pay

    def statistics(self, pay: pd.DataFrame) -> Dict[str, Any]:
        """Calculate matching statistics"""
        all_count = pay.id.count()
        matched = pay[(~pay['status'].str.contains('No Found', na=False)) & (~pay['status'].str.contains('No matching DRs found', na=False))]
        perfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False)]
        relevant = pay[pay['status'].str.contains('relevant', na=False)]
        payout = pay[pay['status'].str.contains('paid out', na=False)]
        noFound = pay[pay['status'].str.contains('No Found', na=False)]
        noMatch = pay[pay['status'].str.contains('No matching DRs found', na=False)]
        
        return {
            'total': all_count,
            'matched': matched.id.count(),
            'matched_rate': matched.id.count() / all_count * 100 if all_count > 0 else 0,
            'perfect_matched': perfect.id.count(),
            'perfect_rate': perfect.id.count() / all_count * 100 if all_count > 0 else 0,
            'relevant_matched': relevant.id.count(),
            'relevant_rate': relevant.id.count() / all_count * 100 if all_count > 0 else 0,
            'paid_out': payout.id.count(),
            'paid_out_rate': payout.id.count() / all_count * 100 if all_count > 0 else 0,
            'unmatched': noFound.id.count() + noMatch.id.count(),
            'unmatched_rate': (noFound.id.count() + noMatch.id.count()) / all_count * 100 if all_count > 0 else 0
        }