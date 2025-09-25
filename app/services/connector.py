from typing import Dict, Any, Optional, Tuple
import pandas as pd
from .utils import fetchFromDB, check_eq, pick_first, check_full_parts_match, list_deduplicate, as_id_list, tz_convert
from pandas.api.types import is_datetime64tz_dtype # type: ignore
from .processor import Processor

class Connector(Processor):
    def __init__(self) -> None:
        super().__init__()
        self.errand_connect_query = self.queries['errandConnect'].iloc[0]
        self.errand_query_condition = [
            "er.\"createdAt\" >= NOW() - INTERVAL '15 day'",
            "er.\"createdAt\" >= NOW() - INTERVAL '3 month' AND er.\"createdAt\" < NOW() - INTERVAL '15 day'"
        ]

    def connect_with_time_windows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Connect emails with errands.
        - If an ErrandDataset is provided, use its preloaded DataFrame.
        - Otherwise, fall back to the existing DB-backed method that fetches.
        """
        df = df.copy()
        windows = getattr(self, 'errand_query_condition', [
            "er.\"createdAt\" >= NOW() - INTERVAL '15 day'",
            "er.\"createdAt\" >= NOW() - INTERVAL '3 month' AND er.\"createdAt\" < NOW() - INTERVAL '15 day'",
        ])

        for condi in windows:
            unmatched_mask = df['errandId'].apply(lambda x: len(x) == 0 if isinstance(x, (list, tuple)) else not x)
            if not unmatched_mask.any():
                break
            errand = self._fetch_and_format_errand(condi)
            
            if errand is None or errand.empty:
                continue
            df = self._single_connect(df, errand)
            
        return df
    
    def _single_connect(self, emails: pd.DataFrame, errands: pd.DataFrame) -> pd.DataFrame:
        if errands is None or errands.empty:
            return emails
        
        df = emails.copy()
        df['errandId'] = df['errandId'].apply(as_id_list)
        unmatched_mask = df['errandId'].apply(lambda x: len(x) == 0 if isinstance(x, (list, tuple)) else not x)
        if unmatched_mask.any():
            sub = df.loc[unmatched_mask].copy() 
            applied = sub.apply(lambda row: self._find_match_for_single_email(row, errands), axis=1, result_type='expand')

            errand_matched = applied.get('errand_matched', pd.Series(False, index=applied.index))
            hit_mask = errand_matched.eq(True) if isinstance(errand_matched, pd.Series) else errand_matched == True
            hit_idx = applied.index[hit_mask]
            if len(hit_idx) > 0:
                cols = [c for c in applied.columns if c not in ('errand_matched', 'errandId')]
                for c in cols:
                    if c in df.columns and is_datetime64tz_dtype(df[c]):
                        s = pd.to_datetime(applied.loc[hit_idx, c], errors='coerce', utc=True)
                        try:
                            s = s.dt.tz_convert(str(df[c].dtype.tz))  # type: ignore
                        except (AttributeError, TypeError):
                            pass
                        df.loc[hit_idx, c] = df.loc[hit_idx, c].combine_first(s)
                    elif c in ['note', 'connectedCol']:
                        if c in df.columns:
                            df.loc[hit_idx, c] = applied.loc[hit_idx, c]
                        else:
                            df[c] = None
                            df.loc[hit_idx, c] = applied.loc[hit_idx, c]
                    else:
                        df.loc[hit_idx, c] = df.loc[hit_idx, c].combine_first(applied.loc[hit_idx, c]) \
                                                if c in df.columns else applied.loc[hit_idx, c]
                if 'errandId' in applied.columns:
                    def _merge_ids(a, b):
                        a = a if isinstance(a, list) else ([] if pd.isna(a) else [a])
                        b = b if isinstance(b, list) else ([] if pd.isna(b) else [b])
                        seen, out = set(), []
                        for x in a + b:
                            if x not in seen:
                                seen.add(x)
                                out.append(x)
                        return out
                    for i in hit_idx:
                        df.at[i, 'errandId'] = _merge_ids(df.at[i, 'errandId'], applied.at[i, 'errandId'])
        df['errandId'] = df['errandId'].apply(as_id_list)

        return df

    def _fetch_and_format_errand(self, condition: str) -> Optional[pd.DataFrame]:
        query = self.errand_connect_query.format(CONDITION=condition)
        errand_df = fetchFromDB(query)
        if errand_df.empty:
            return None

        errand_df = tz_convert(errand_df, 'date')
        text_cols = []
        for col in errand_df.select_dtypes(include=['object', 'string']).columns:
            sample_val = errand_df[col].dropna().iloc[0] if not errand_df[col].dropna().empty else None
            if sample_val is not None and isinstance(sample_val, str):
                text_cols.append(col)

        if text_cols:
            errand_df[text_cols] = errand_df[text_cols].apply(
                lambda col: col.map(lambda v: self.clean_email_text(v) if pd.notna(v) and isinstance(v, str) else v))
        for col in ['animalName', 'ownerName']:
            errand_df[col] = errand_df[col].str.replace(r'[,._\-()/*\s]+', ' ', regex=True) \
                                            .str.replace(r'[^a-zA-ZåäöÅÄÖ\'"´ ]', '', regex=True) \
                                            .str.strip()
        return errand_df
           
    def _errand_row_filter(self, row, email_sender, email_receiver):
        # if row['reference'] == '1000725927':
        sender_match = (
            pd.notna(row['sender']) and pd.isna(row['receiver']) and pd.notna(email_sender) and
            check_eq(row['sender'], email_sender))
            
        receiver_match = (
            pd.isna(row['sender']) and pd.notna(row['receiver']) and pd.notna(email_receiver) and
            check_eq(row['receiver'], email_receiver))
        
        both_match = (
            pd.notna(row['sender']) and pd.notna(row['receiver']) and
            pd.notna(email_sender) and pd.notna(email_receiver) and
            check_eq(row['sender'], email_sender) and check_eq(row['receiver'], email_receiver))
        
        empty_match = pd.isna(row['sender']) and pd.isna(row['receiver'])

        return sender_match or receiver_match or both_match or empty_match 
        
    def _filter_candidate_errand(self, email_row: pd.Series, errand: pd.DataFrame) -> pd.DataFrame:
        cand = errand[errand['date'] <= email_row['date']].copy() 
        if cand.empty: return cand
        
        email_source, email_sendTo   = email_row.get('source'), email_row.get('sendTo')
        email_sender = email_row.get('sender') if (pd.notna(email_row.get('sender'))) and (email_row.get('sender') not in ['DRP','Wisentic','Provet_Cloud']) else None
        email_receiver= email_row.get('receiver') if (pd.notna(email_row.get('receiver'))) and (email_row.get('receiver') not in ['DRP','Wisentic']) else None
        cand = cand.assign(sender=pd.NA, receiver=pd.NA)
        if pd.notna(email_sender) and (email_source == 'Insurance_Company') and (email_sendTo == 'Clinic'):
            cand.loc[:, 'sender'] = cand['insuranceCompany']
            if pd.notna(email_receiver):
                cand.loc[:, 'receiver'] = cand['clinicName']
        elif pd.notna(email_sender) and (email_source == 'Clinic') and (email_sendTo == 'Insurance_Company'):
            cand.loc[:, 'sender'] = cand['clinicName']
            if pd.notna(email_receiver):
                cand.loc[:, 'receiver'] = cand['insuranceCompany']
        mask = cand.apply(lambda row: self._errand_row_filter(row, email_sender, email_receiver), axis=1)
        # if not mask.any():
        return cand[mask]

    def _find_match_for_single_email(self, email_row: pd.Series, errand: pd.DataFrame) -> Dict[str, Any]:
        matched_errand, connected_col, note = self._match_by_reference(email_row, errand)
        if matched_errand is not None:
            result_dict = self._fill_back_result(email_row, matched_errand, connected_col or "", note or "")
            return result_dict

        cand = self._filter_candidate_errand(email_row, errand)

        if not cand.empty:
            matched_errand, connected_col, note = self._match_by_number(email_row, cand)
            if matched_errand is None:
                matched_errand, connected_col, note = self._match_by_name(email_row, cand)
            if matched_errand is not None:
                result_dict = self._fill_back_result(email_row, matched_errand, connected_col or "", note or "")
                return result_dict

        # Return original email data with unmatched status
        # Preserve original errandId if it exists
        cur_ids = email_row.get('errandId')
        if not isinstance(cur_ids, list):
            cur_ids = [] if pd.isna(cur_ids) else [cur_ids]

        result = {
            "errand_matched": False,
            "errandId": cur_ids
        }

        return result
        
    def _match_by_reference(self, email_row: pd.Series, errand: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str], Optional[str]]:
        ref = email_row.get('reference')
        if pd.notna(ref) and 'reference' in errand.columns:
            m = errand[errand['reference'] == ref]
            hit = pick_first(m)
            if hit is not None:
                return hit, 'reference', 'Reliable'
        return None, None, None
    
    def _match_by_number(self, email_row: pd.Series, cand: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str], Optional[str]]:
        email_settle = email_row.get('settlementAmount')
        email_total  = email_row.get('totalAmount')
        
        for col in ['insuranceNumber', 'damageNumber']:
            email_val = email_row.get(col)
            if pd.isna(email_val) or col not in cand.columns:
                continue

            mask_full_match = cand[col] == email_val
            if col == 'damageNumber': 
                # remove the last part for avoiding overmatching(last part is usually a small number,e.g. 1)
                mask_part_match = cand[col].apply(lambda v: str(email_val) in str(v).split('-') if pd.notna(v) else False)
                possible = cand[mask_full_match | mask_part_match]
            else:
                possible = cand[cand[col] == email_val]

            if pd.isna(email_settle) and pd.isna(email_total):
                hit = pick_first(possible)
                if hit is not None:
                    return hit, col, 'Unreliable'

            if pd.notna(email_settle) and 'settlementAmount' in possible.columns:
                m_splittle = possible[possible['settlementAmount'].notna() &
                                    (possible['settlementAmount'] == email_settle)]
                hit = pick_first(m_splittle)
                if hit is not None:
                    return hit, col, 'Reliable'

            if pd.notna(email_total) and 'totalAmount' in possible.columns:
                m_total = possible[possible['totalAmount'].notna() &
                                (possible['totalAmount'] == email_total)]
                hit = pick_first(m_total)
                if hit is not None:
                    return hit, col, 'Reliable'

        return None, None, None

    def _match_by_name(self, email_row: pd.Series, cand: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str], Optional[str]]:
        email_animal = email_row.get('animalName')
        email_owner  = email_row.get('ownerName')
        email_settle = email_row.get('settlementAmount')
        email_total  = email_row.get('totalAmount')

        if pd.isna(email_animal) and pd.isna(email_owner):
            return None, None, None

        base = cand[(cand['animalName'].notna()) | (cand['ownerName'].notna())]
        if base.empty:
            return None, None, None
        settle_sub = base[(base['settlementAmount'].notna()) & (base['settlementAmount'] == email_settle)] if pd.notna(email_settle) else None
        total_sub  = base[(base['totalAmount'].notna())      & (base['totalAmount'] == email_total)]       if pd.notna(email_total)  else None
        
        name_cols = ['animalName', 'ownerName']
        full = {c: [] for c in name_cols}
        part = {c: [] for c in name_cols}

        if pd.isna(email_settle) and pd.isna(email_total):
            fa, pa = check_full_parts_match(base, 'animalName', email_animal)
            fo, po = check_full_parts_match(base, 'ownerName',  email_owner)
            full['animalName'] += fa;  part['animalName'] += pa
            full['ownerName']  += fo;  part['ownerName']  += po

        if pd.notna(email_settle):
            fa, pa = check_full_parts_match(settle_sub, 'animalName', email_animal)
            fo, po = check_full_parts_match(settle_sub, 'ownerName',  email_owner)
            full['animalName'] += fa;  part['animalName'] += pa
            full['ownerName']  += fo;  part['ownerName']  += po

        if pd.notna(email_total):
            fa, pa = check_full_parts_match(total_sub, 'animalName', email_animal)
            fo, po = check_full_parts_match(total_sub, 'ownerName',  email_owner)
            full['animalName'] += fa;  part['animalName'] += pa
            full['ownerName']  += fo;  part['ownerName']  += po

        for k in name_cols:
            full[k] = list_deduplicate(full[k])
            part[k] = list_deduplicate(part[k])

        animal_matches = set(full['animalName']) | set(part['animalName'])
        owner_matches  = set(full['ownerName'])  | set(part['ownerName'])

        if animal_matches and owner_matches:
            common_ids = list(animal_matches & owner_matches)
            common_ids = list_deduplicate(common_ids)
            if len(common_ids) > 1:
                sub = cand[cand['errandId'].isin(common_ids)]
                best = sub.sort_values('date', ascending=False).iloc[0]
                return best, 'latestCommonName', 'Unreliable'
            elif len(common_ids) == 1:
                best = cand[cand['errandId'] == common_ids[0]].iloc[0]
                if pd.isna(email_settle) and pd.isna(email_total):
                    return best, 'singleCommonName', 'Reliable'
                
                has_settlement_match = (pd.notna(email_settle) and 
                                      pd.notna(best.get('settlementAmount')) and 
                                      best['settlementAmount'] == email_settle)
                has_total_match = (pd.notna(email_total) and 
                                 pd.notna(best.get('totalAmount')) and 
                                 best['totalAmount'] == email_total)
                
                if has_settlement_match or has_total_match:
                    return best, 'singleCommonName', 'Reliable'
                else:
                    return best, 'singleCommonName', 'Unreliable'

        if len(full['animalName']) == 1 and len(full['ownerName']) == 0:
            best = cand[cand['errandId'] == full['animalName'][0]].iloc[0]
            return best, 'animalFullName', 'Unreliable'

        if len(full['ownerName']) == 1 and len(full['animalName']) == 0:
            best = cand[cand['errandId'] == full['ownerName'][0]].iloc[0]
            return best, 'ownerFullName', 'Unreliable'

        return None, None, None

    def _fill_back_result(self, email_row: pd.Series, matched_errand: pd.Series, connected_col: str, note: str) -> Dict[str, Any]:
        res: Dict[str, Any] = {"errand_matched": True}
        cur_ids = email_row.get('errandId')
        if not isinstance(cur_ids, list):
            cur_ids = [] if pd.isna(cur_ids) else [cur_ids]
        
        res['errandId'] = cur_ids + [int(matched_errand['errandId'])]
        res['insuranceCaseRef'] = matched_errand.get('reference') 
        res['errandDate']    = matched_errand.get('date')
        res['connectedCol']  = connected_col
        res['note']          = note
        if matched_errand.get('paymentOption') is not None:
            res['paymentOption'] = matched_errand.get('paymentOption')
        if matched_errand.get('strategyType') is not None:    
            res['strategyType']  = matched_errand.get('strategyType')
        res['errand_matched']      = True
        
        if pd.isna(email_row['originReceiver']) or email_row['originReceiver'] in ['DRP', 'Wisentic']:
            if email_row['source'] == 'Clinic' and pd.notna(matched_errand['insuranceCompany']):
                res['receiver'] = matched_errand['insuranceCompany']
            elif email_row['source'] == 'Insurance_Company' and pd.notna(matched_errand['clinicName']):
                res['receiver'] = matched_errand['clinicName']
                
        if email_row['originSender'] in ['Wisentic','DRP'] and pd.notna(matched_errand['insuranceCompany']):
            res['sender'] = matched_errand['insuranceCompany']
        elif email_row['sender']=='Provet_Cloud' and pd.notna(matched_errand['clinicName']):
            res['sender'] = matched_errand['clinicName']
        

        return res


