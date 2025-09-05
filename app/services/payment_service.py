import regex as reg
import pandas as pd
from typing import List, Dict, Any, Optional
from itertools import combinations
from .base_service import BaseService
from .utils import get_payoutEntity, fetchFromDB


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
        self.payment_query = self.queries['payment'].iloc[0] 
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

    def process_payment(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Process payment data - extract references and initialize columns"""
        pay['createdAt'] = pd.to_datetime(pay['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm') 

        # Use pre-compiled regex pattern
        pay.loc[pay['reference'].notna(),'extractReference'] = pay.loc[pay['reference'].notna(),'reference'].apply(
            lambda x: ''.join(self.ref_reg.findall(x)) if isinstance(x, str) else None
        )
        pay.loc[pay['extractReference'].notna(),'extractReference'] = pay.loc[pay['extractReference'].notna(),'extractReference'].replace('', None)
        pay['settlementAmount'] = 0
        pay['status'] = ""

        # Initialize columns for info parsing
        for col in self.info_item_list:
            colName = col.split('_')[1]
            if colName not in pay.columns:
                pay[colName] = None
        
        # Initialize list columns
        init_columns = ['valPay', 'valErrand', 'isReference', 'insuranceCaseId', 'referenceLink']
        for col in init_columns:
            pay[col] = [[] for _ in range(len(pay))]
        
        return pay[['id','valPay','valErrand','amount','settlementAmount','isReference','insuranceCaseId','referenceLink',
                    'status','extractReference','extractDamageNumber','extractOtherNumber','bankName','info','reference','createdAt']]

    def parse_info(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Parse payment info field using regex patterns - optimized with pre-compiled patterns"""
        mask = pay['info'].notna()
        for idxPay, rowPay in pay[mask].iterrows():
            ic = self.bank_dict.get(rowPay['bankName'], 'None') 
            mask_info = self.info_reg['item'].str.startswith(ic)
            for _, rowInfoReg in self.info_reg[mask_info].iterrows():
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

    def find_matches(self, pay: pd.DataFrame, errand: pd.DataFrame, idxPay: Any, rowPay: pd.Series) -> List[int]:
        """Find matching insurance case IDs for a payment - optimized with vectorized operations"""
        matched = {colPay: [] for colPay in self.matching_cols_pay}
        
        # Filter errands by date first
        errand_filtered = errand[errand['createdAt'] <= rowPay['createdAt']].copy()
        if errand_filtered.empty:
            return []
        
        # Fill NaN settlement amounts
        errand_filtered['settlementAmount'] = errand_filtered['settlementAmount'].fillna(0)
        
        for colPay in self.matching_cols_pay:
            valPay = rowPay[colPay]
            if pd.notna(valPay):
                valAmount = rowPay['amount']
                str_valPay = str(valPay)
                
                # Create boolean masks for all errand columns at once
                matches_mask = pd.Series(False, index=errand_filtered.index)
                for colErrand in self.matching_cols_errand:
                    col_mask = (errand_filtered[colErrand].notna() & 
                               (errand_filtered[colErrand].astype(str) == str_valPay))
                    matches_mask |= col_mask
                
                if matches_mask.any():
                    matched_errands = errand_filtered[matches_mask]
                    
                    # Update pay DataFrame with matched references (vectorized)
                    current_refs = set(str(ref) for ref in pay.at[idxPay, 'isReference'])
                    new_refs = matched_errands['isReference'].astype(str).tolist()
                    for ref in new_refs:
                        if ref not in current_refs:
                            pay.at[idxPay, 'isReference'].append(ref)
                            pay.at[idxPay, 'valPay'].append(str_valPay)
                            current_refs.add(ref)
                    
                    # Find amount matches (vectorized)
                    amount_mask = (matched_errands['settlementAmount'] == valAmount)
                    if amount_mask.any():
                        matched_case_ids = matched_errands[amount_mask]['insuranceCaseId'].astype(int).tolist()
                        matched[colPay].extend([cid for cid in matched_case_ids if cid not in matched[colPay]])
                        
                        # Update valErrand list
                        matched_vals = matched_errands[amount_mask][self.matching_cols_errand].values.flatten()
                        for val in matched_vals:
                            if pd.notna(val):
                                pay.at[idxPay, 'valErrand'].append(str(val))
        
        # Optimize set operations
        matchedLists = [set(matched[colPay]) for colPay in self.matching_cols_pay if matched[colPay]]
        if matchedLists:
            # Get intersection first, then union if needed
            matchedInsuranceCaseID = list(set.intersection(*matchedLists)) if len(matchedLists) > 1 else list(matchedLists[0])
            if not matchedInsuranceCaseID:
                matchedInsuranceCaseID = list(set.union(*matchedLists))
        else:
            matchedInsuranceCaseID = []
        
        return matchedInsuranceCaseID

    def generate_links(self, colList1: List[Any], colList2: List[Any], condition: str) -> List[str]:
        """Generate HTML links for matched references - optimized with batch query"""
        links = []
        if len(colList1) == 0:
            return links
            
        # Batch query instead of individual queries
        if condition == 'ic.reference':
            # Create IN clause for batch query
            ids_str = ', '.join([f"'{id}'" for id in colList1])
            condition_sql = f"{condition} IN ({ids_str})"
        elif condition == 'ic.id':
            # Create IN clause for batch query  
            ids_str = ', '.join([str(id) for id in colList1])
            condition_sql = f"{condition} IN ({ids_str})"   
        else:
            return links
            
        # Single batch query
        try:
            result = fetchFromDB(self.errand_link_query.format(CONDITION=condition_sql))
            # Create lookup dictionary for fast access
            if not result.empty:
                if condition == 'ic.reference':
                    # Don't include 'reference' in columns selection since it's now the index
                    lookup = result.set_index('reference')[['errandNumber']].to_dict('index')
                    # Add reference back to each row data
                    for ref_key in lookup:
                        lookup[ref_key]['reference'] = ref_key
                else:
                    lookup = result.set_index('id')[['errandNumber', 'reference']].to_dict('index')

                # Generate links in order
                for id, valErrand in zip(colList1, colList2):
                    if id in lookup:
                        row_data = lookup[id]
                        errandNumber = row_data['errandNumber']
                        ref = row_data['reference'] 
                        link = f'<a href="{self.base_url}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by {valErrand}">{ref}</a>'
                        links.append(link)
                    else:
                        links.append(f'{id} (No Corresponding Link)')
            else:
                # No results found
                links = [f'{id} (No Corresponding Link)' for id in colList1]
                
        except Exception as e:
            # Fallback to individual queries if batch fails
            print(f"Batch query failed, falling back to individual queries: {e}")
            for id, valErrand in zip(colList1, colList2):
                if condition == 'ic.reference':
                    condition_sql = f"{condition} = '{id}'"
                elif condition == 'ic.id':
                    condition_sql = f"{condition} = {id}"
                try:
                    result = fetchFromDB(self.errand_link_query.format(CONDITION=condition_sql))
                    if not result.empty:
                        errandNumber = result.iloc[0]['errandNumber']
                        ref = result.iloc[0]['reference']
                        link = f'<a href="{self.base_url}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by {valErrand}">{ref}</a>'
                        links.append(link)
                    else:
                        links.append(f'{id} (No Corresponding Link)')
                except Exception:
                    links.append(f'{id} (Query Error)')
                    
        return links

    def match_by_info(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Match payments by info/reference data"""
        mask = (pay['info'].notna() | pay['extractReference'].notna())
        for idxPay, rowPay in pay[mask].iterrows():
            matchedInsuranceCaseID = self.find_matches(pay, errand, idxPay, rowPay) 
            qty = len(matchedInsuranceCaseID)
            if qty > 0:
                pay.at[idxPay, 'insuranceCaseId'].extend(matchedInsuranceCaseID) 
                links = self.generate_links(rowPay['insuranceCaseId'], rowPay['valErrand'], 'ic.id')
                pay.at[idxPay, 'referenceLink'] = links
                if qty == 1:
                    pay.at[idxPay, 'status'] = f"One DR matched perfectly (reference: {', '.join(links)})."
                elif qty > 1:
                    pay.at[idxPay, 'status'] = f"Found {qty} matching DRs (references: {', '.join(links)}) and the payment amount matches each one."
            else:      
                pay.at[idxPay, 'status'] = "No Found" 

        return pay

    def partly_amount_matching(self, refAmountDict: Dict[str, float], target_amount: float) -> Optional[List[str]]:
        """Find combinations of references that match the target amount"""
        references = list(refAmountDict.keys())
        amounts = list(refAmountDict.values())
        
        for r in range(1, len(amounts) + 1):
            for combo in combinations(zip(references, amounts), r):
                combo_references, combo_amounts = zip(*combo)
                if sum(combo_amounts) == target_amount:
                    return list(combo_references)
                
        return None

    def reminder_unmatched_amount(self, pay: pd.DataFrame) -> pd.DataFrame:
        """Handle unmatched payments by checking all errands"""
        mask = (pay['status'].isin(["No Found",""]))
        for idx, rowPay in pay[mask].iterrows():
            matchedInsuranceCaseID, isReference, links = [], [], []
            refAmountDict = {}
            msg = "No Found"
            
            # Collect all possible references
            if (len(rowPay['isReference']) > 0):
                for ref in rowPay['isReference']:
                    if ref not in isReference:
                        isReference.append(ref)
            if pd.notna(rowPay['extractReference']) and (len(rowPay['extractReference']) == 10) and (rowPay['extractReference'] not in isReference):
                isReference.append(str(rowPay['extractReference']))                 
            if pd.notna(rowPay['extractOtherNumber']) and (len(rowPay['extractOtherNumber']) == 10) and (rowPay['extractOtherNumber'] not in isReference):
                isReference.append(str(rowPay['extractOtherNumber']))
            if pd.notna(rowPay['extractDamageNumber']) and (len(rowPay['extractDamageNumber']) == 10) and (rowPay['extractDamageNumber'] not in isReference):
                isReference.append(str(rowPay['extractDamageNumber']))
                
            if len(isReference) > 0:
                condition_sql = f"""AND ic.reference IN ({', '.join([f"'{ref}'" for ref in set(isReference)])})"""  
                subErrand = fetchFromDB(self.errand_pay_query.format(CONDITION=condition_sql))

                subErrand.loc[subErrand['settlementAmount'].isna(), 'settlementAmount'] = 0
                if not subErrand.empty:
                    for _, rowSub in subErrand.iterrows():
                        if rowSub['insuranceCaseId'] not in matchedInsuranceCaseID:
                            settlement_amount = rowSub['settlementAmount'] if pd.notna(rowSub['settlementAmount']) else 0
                            pay.at[idx, 'settlementAmount'] += float(settlement_amount)
                            matchedInsuranceCaseID.append(rowSub['insuranceCaseId'])
                            
                        if str(rowSub['isReference']) not in [str(ref) for ref in pay.at[idx, 'isReference']]:
                            pay.at[idx, 'isReference'].append(str(rowSub['isReference']))
                            pay.at[idx, 'valPay'].append(str(rowSub['isReference']))
                            
                        if str(rowSub['isReference']) not in refAmountDict:
                            settlement_amount = rowSub['settlementAmount'] if pd.notna(rowSub['settlementAmount']) else 0
                            refAmountDict[str(rowSub['isReference'])] = float(settlement_amount)

            refList = pay.at[idx, 'isReference']
            valErrandList = pay.at[idx, 'valPay']
            links = self.generate_links(refList, valErrandList, 'ic.reference')
            pay.at[idx, 'referenceLink'] = links
            
            qty = len(matchedInsuranceCaseID)
            if qty > 0:
                totalSettlementAmount = pay.at[idx, 'settlementAmount']
                rowPaymentAmount = rowPay['amount']

                if rowPaymentAmount == totalSettlementAmount:
                    if qty == 1:
                        msg = f"One DR matched perfectly (reference: {', '.join(links)})."
                    else:       
                        msg = f"Found {qty} matching DRs (references: {', '.join(links)}), and the total amount matches the payment."
                else:
                    matched_references = self.partly_amount_matching(refAmountDict, rowPaymentAmount)
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

    def match_entity_and_amount(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """Match by entity (bank/clinic) and amount"""
        def msg_for_one(oneLineDF: pd.DataFrame, source: str, amount: float) -> str:
            errandNumber = oneLineDF.iloc[0]['errandNumber']
            ref = oneLineDF.iloc[0]['isReference']
            settlement_amount = oneLineDF.iloc[0]['settlementAmount'] if pd.notna(oneLineDF.iloc[0]['settlementAmount']) else 0
            
            # Debug for payment 63874
            if amount == 70500:
                print(f"      msg_for_one: amount={amount}, settlement={settlement_amount}, match={amount == float(settlement_amount)}")
            
            if source == 'Insurance_Company':
                entity = oneLineDF.iloc[0]['insuranceCompanyName'] 
            elif source == 'Clinic':
                entity = oneLineDF.iloc[0]['clinicName']
                
            link = f'<a href="{self.base_url}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>'
            
            if amount == float(settlement_amount):
                msg = f"One DR matched perfectly (reference: {link}) by both entity and amount."
            else:
                msg = "No Found"
                
            return msg
                
        # Use cached entity dictionaries
        payoutEntitySource, icDict, clinicDict = self._get_entity_dicts()
        
        mask = (pay['status'].isin(["No Found",""]))
        for idx, rowPay in pay[mask].iterrows():
            icList, clinicList = [], []
            amount = rowPay['amount']
            msg = "No Found"  # Default message
            
            # Debug logging for payment 63874
            if rowPay['id'] == 63874:
                print(f"\n🔍 DEBUG Payment {rowPay['id']}:")
                print(f"   Bank: {rowPay['bankName']}")
                print(f"   Amount: {amount}")
                print(f"   Status: {rowPay['status']}")
            
            source = payoutEntitySource.get(rowPay['bankName'], "Unknown")
            
            if rowPay['id'] == 63874:
                print(f"   Source lookup result: {source}")
                print(f"   Available payout entities: {list(payoutEntitySource.keys())[:10]}...")
                print(f"   Bank dict mapping: {self.bank_dict.get(rowPay['bankName'], 'NOT_FOUND')}")
                print(f"   Available bank dict keys: {list(self.bank_dict.keys())[:5]}...")
            if source == 'Insurance_Company':
                icList = icDict.get(rowPay['bankName'], [])
                if rowPay['id'] == 63874:
                    print(f"   Insurance Company List: {icList}")
            elif source == 'Clinic':
                clinicList = clinicDict.get(rowPay['bankName'], [])
                if rowPay['id'] == 63874:
                    print(f"   Clinic List: {clinicList}")
            else:
                if rowPay['id'] == 63874:
                    print(f"   ERROR: Unknown source '{source}', trying bank_dict fallback...")
                
                # Fallback: Use bank_dict mapping for insurance companies
                bank_ref = self.bank_dict.get(rowPay['bankName'])
                if bank_ref:
                    # Map common insurance company references to lowercase names for matching
                    ic_name_mapping = {
                        'folksam': 'folksam',
                        'agria': 'agria', 
                        'sveland': 'sveland',
                        'if': 'if',
                        'trygghansa': 'trygg-hansa',
                        'dina': 'dina'
                    }
                    expected_ic_name = ic_name_mapping.get(bank_ref.lower())
                    if expected_ic_name:
                        source = 'Insurance_Company'
                        icList = [expected_ic_name]
                        if rowPay['id'] == 63874:
                            print(f"   Fallback successful: {bank_ref} → {expected_ic_name} (lowercase)")
                    else:
                        if rowPay['id'] == 63874:
                            print(f"   No mapping found for bank_ref: {bank_ref}")
                        continue
                else:
                    if rowPay['id'] == 63874:
                        print(f"   No bank_dict mapping found, skipping...")
                    continue
            
            # Use case-insensitive matching for insurance company names
            if source == 'Insurance_Company':
                entityMatched = errand.loc[
                    (errand['createdAt'] <= rowPay['createdAt']) & 
                    (errand['insuranceCompanyName'].str.lower().isin([name.lower() for name in icList]))
                ]
            else:  # Clinic
                entityMatched = errand.loc[
                    (errand['createdAt'] <= rowPay['createdAt']) & 
                    (errand['clinicName'].isin(clinicList))
                ]
            qty = entityMatched.shape[0]
            
            if rowPay['id'] == 63874:
                print(f"   Entity matched errands: {qty}")
                if qty > 0:
                    # Debug: Show available columns
                    print(f"   Available columns: {entityMatched.columns.tolist()}")
                    
                    # Check if errand 74806 is in the matched results using correct column name
                    errand_id_col = 'errandId' if 'errandId' in entityMatched.columns else 'insuranceCaseId'
                    errand_ids = entityMatched[errand_id_col].tolist()
                    print(f"   Matched errand IDs: {errand_ids}")
                    print(f"   Settlement amounts: {entityMatched['settlementAmount'].tolist()}")
                    
                    if 74806 in errand_ids:
                        print(f"   ✅ FOUND: Errand 74806 IS in matched results!")
                        idx_74806 = errand_ids.index(74806)
                        print(f"   Errand 74806 amount: {entityMatched['settlementAmount'].tolist()[idx_74806]}")
                    else:
                        print(f"   ❌ Errand 74806 NOT in matched results")
                        
                        # Check if 74806 exists at all in the full errand dataset
                        full_errand_id_col = 'errandId' if 'errandId' in errand.columns else 'insuranceCaseId'
                        all_case_ids = errand[full_errand_id_col].tolist()
                        if 74806 in all_case_ids:
                            print(f"   ℹ️ But errand 74806 EXISTS in full dataset")
                            errand_74806 = errand[errand[full_errand_id_col] == 74806]
                            if not errand_74806.empty:
                                row_74806 = errand_74806.iloc[0]
                                print(f"   Errand 74806 details:")
                                print(f"     Insurance Company: {row_74806['insuranceCompanyName']}")
                                print(f"     Settlement Amount: {row_74806['settlementAmount']}")
                                print(f"     Created At: {row_74806['createdAt']}")
                                print(f"     Payment Created At: {rowPay['createdAt']}")
                                print(f"     Date check: {row_74806['createdAt']} <= {rowPay['createdAt']} = {row_74806['createdAt'] <= rowPay['createdAt']}")
                                print(f"     Company check: '{row_74806['insuranceCompanyName'].lower()}' in {[name.lower() for name in icList]} = {row_74806['insuranceCompanyName'].lower() in [name.lower() for name in icList]}")
                        else:
                            print(f"   ❌ Errand 74806 does NOT exist in full dataset")
                else:
                    # Check what's available for debugging
                    print(f"   Total errands in date range: {len(errand[errand['createdAt'] <= rowPay['createdAt']])}")
                    if source == 'Insurance_Company':
                        insurance_matches = errand[errand['insuranceCompanyName'].isin(icList)]
                        print(f"   Errands with matching insurance companies: {len(insurance_matches)}")
                        print(f"   Sample insurance company names in errand data: {errand['insuranceCompanyName'].unique()[:10].tolist()}")
            if qty == 1:
                msg = msg_for_one(entityMatched, source, amount)
                    
            elif qty > 1:
                if rowPay['id'] == 63874:
                    print(f"   Processing {qty} matched errands in groups...")
                    # Find which group contains errand 74806
                    sameClinicAnimalGroup_debug = entityMatched.groupby(['insuranceCompanyName','clinicName','animalId'])
                    errand_id_col = 'errandId' if 'errandId' in entityMatched.columns else 'insuranceCaseId'
                    for group_name, group_df_debug in sameClinicAnimalGroup_debug:
                        if 74806 in group_df_debug[errand_id_col].tolist():
                            print(f"   🎯 FOUND 74806 in group: {group_name}")
                            print(f"   Group contains errands: {group_df_debug[errand_id_col].tolist()}")
                            print(f"   Group amounts: {group_df_debug['settlementAmount'].tolist()}")
                            break
                    print(f"   Total number of groups: {len(list(sameClinicAnimalGroup_debug))}")
                    
                sameClinicAnimalGroup = entityMatched.groupby(['insuranceCompanyName','clinicName','animalId']) 
                group_count = 0
                for _, group_df in sameClinicAnimalGroup:
                    group_count += 1
                    if rowPay['id'] == 63874:
                        print(f"   Group {group_count}: {len(group_df)} errands, amounts: {group_df['settlementAmount'].tolist()}")
                    if group_df.shape[0] == 1:
                        temp_msg = msg_for_one(group_df, source, amount)
                        if temp_msg != "No Found":
                            msg = temp_msg
                            break  # Found a successful match, no need to continue
                        # Continue to next group if this one didn't match
                    else:
                        refAmountDict, links = {}, []
                        for _, row in group_df.iterrows():
                            settlement_amount = row['settlementAmount'] if pd.notna(row['settlementAmount']) else 0
                            refAmountDict[row['isReference']] = float(settlement_amount)
                            errandNumber = row['errandNumber']
                            ref = row['isReference']
                            if source == 'Insurance_Company':
                                entity = row['insuranceCompanyName'] 
                            elif source == 'Clinic':
                                entity = row['clinicName']
                            else:
                                continue
                            
                            link = f'<a href="{self.base_url}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>'
                            links.append((ref, link))
                        
                        matched_references = self.partly_amount_matching(refAmountDict, amount)
                        if matched_references:
                            matched_links = [link for ref, link in links if ref in matched_references]
                            if len(matched_references) == 1:
                                msg = f"One DR matched perfectly (reference: {', '.join(matched_links)}) by both entity and amount."
                            else:
                                msg = f"Found {len(matched_references)} matching DRs (references: {', '.join(matched_links)}) by entity, and the total amount matches the payment."
                            break  # Found a successful match, no need to continue
                        # Continue to next group if this one didn't match
            
            pay.at[idx, 'status'] = msg

        return pay

    def match_payout(self, pay: pd.DataFrame, payout: pd.DataFrame) -> pd.DataFrame:
        """Match against payout records - optimized with vectorized operations"""
        if payout.empty:
            mask = (pay['status'].isin(["No Found",""]))
            pay.loc[mask, 'status'] = 'No matching DRs found.'
            return pay
            
        # Prepare payout data
        payout_clean = payout.copy()
        payout_clean['reference'] = payout_clean['reference'].astype(str)
        
        mask = (pay['status'].isin(["No Found",""]))
        for idx, rowPay in pay[mask].iterrows():
            matchedTransId, matchedClinicName, matchedType = set(), set(), set()
            
            # Get all payment values to match
            pay_values = []
            for col in self.matching_cols_pay:
                valPay = rowPay[col]
                if pd.notna(valPay):
                    pay_values.append(str(valPay))
            
            if pay_values:
                # Vectorized matching - find all matches at once
                ref_mask = payout_clean['reference'].isin(pay_values)
                amount_mask = (payout_clean['amount'] == rowPay['amount'])
                combined_mask = ref_mask & amount_mask
                
                if combined_mask.any():
                    matched_payouts = payout_clean[combined_mask]
                    
                    # Extract unique values efficiently
                    trans_ids = matched_payouts['transactionId'].dropna().astype(int).unique()
                    clinic_names = matched_payouts['clinicName'].dropna().unique() 
                    types = matched_payouts['type'].dropna().unique()
                    
                    matchedTransId.update(trans_ids)
                    matchedClinicName.update(clinic_names)
                    matchedType.update(types)

            qty = len(matchedTransId)
            if qty == 1:
                pay.at[idx, 'status'] = f"Payment has been paid out<br>             TransactionId: {list(matchedTransId)[0]}<br>             Amount: {rowPay['amount'] / 100:.2f} kr<br>             Clinic Name: {list(matchedClinicName)[0]}<br>             Type: {list(matchedType)[0]}" 
            elif qty > 0:
                pay.at[idx, 'status'] = f"Payment has been paid out {qty} times<br>    TransactionId:{' '.join(map(str, sorted(matchedTransId)))}<br>             Amount: {rowPay['amount'] / 100:.2f} kr<br>    Clinic Name: {' '.join(sorted(matchedClinicName))}<br>    Type: {' '.join(sorted(matchedType))}"
            else:
                pay.at[idx, 'status'] = 'No matching DRs found.'   

        return pay

    def calculate_statistics(self, pay: pd.DataFrame) -> Dict[str, Any]:
        """Calculate matching statistics"""
        all_count = pay.id.count()
        matched = pay[(~pay['status'].str.contains('No Found', na=False)) & (~pay['status'].str.contains('No matching DRs found', na=False))]
        perfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False)]
        payout = pay[pay['status'].str.contains('paid out', na=False)]
        noFound = pay[pay['status'].str.contains('No Found', na=False)]
        noMatch = pay[pay['status'].str.contains('No matching DRs found', na=False)]
        
        # Print all perfect matched payment IDs
        perfect_ids = perfect['id'].tolist()
        print(f"\n🎯 PERFECT MATCHED PAYMENT IDs ({len(perfect_ids)} total):")
        print(f"   {sorted(perfect_ids)}")
        
        return {
            'total': all_count,
            'matched': matched.id.count(),
            'matched_rate': matched.id.count() / all_count * 100 if all_count > 0 else 0,
            'perfect_matched': perfect.id.count(),
            'perfect_rate': perfect.id.count() / all_count * 100 if all_count > 0 else 0,
            'paid_out': payout.id.count(),
            'paid_out_rate': payout.id.count() / all_count * 100 if all_count > 0 else 0,
            'unmatched': noFound.id.count() + noMatch.id.count(),
            'unmatched_rate': (noFound.id.count() + noMatch.id.count()) / all_count * 100 if all_count > 0 else 0
        }