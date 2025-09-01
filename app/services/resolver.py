import pandas as pd
import regex as reg
from dataclasses import dataclass
from typing import Dict, Optional, Any, List, Tuple
from .base_service import BaseService
from .utils import (fetchFromDB, 
                    base_match, 
                    lower_and_split, 
                    parse_email_address,
                    extract_first_address, 
                    expand_matching_clinic, 
                    get_staffAnimal
                    )


fb_name_mapping = {
                'Trygghansa': 'Trygg-Hansa',
                'Mjoback': 'Mjöbäcks Pastorat Hästförsäkring',
                'Manypets': 'Many Pets',
                'Moderna': 'Moderna Försäkringar',
                'Dina': 'Dina Försäkringar',
                'Ica': 'ICA Försäkring'
            }

class SenderResolver(BaseService):  
    def detect_sender(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main method to detect sender information
        
        Args:
            df: Input DataFrame with 'from' column
            
        Returns:
            DataFrame with added columns: source, originSender, sender
        """
        df['source'], df['originSender'] = 'Other', None
        df['parsedFrom'] = df['from'].apply(extract_first_address)

        df = self.set_sender_fb(df)        
        df = self.set_sender_clinic(df)    
        df = self.set_sender_drp(df)       
        df = self.set_sender_finance(df)   
        df = self.set_sender_postmark(df)          
        df['sender'] = df['originSender']

        return df  

    def set_sender_fb(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = ((df['source'] == 'Other') & (df['parsedFrom'].str.lower().str.contains(self.fb_ref_str, na=False)))
        if mask.any():
            mask_unmatched = df['originSender'].isna()
            df.loc[mask, 'source'] = 'Insurance_Company'
            df.loc[mask & mask_unmatched, 'originSender'] = (
                df.loc[mask & mask_unmatched, 'parsedFrom']
                .apply(lambda x: next((item.capitalize() for item in self.fb_ref_list 
                                     if isinstance(x, str) and item in x), None))
            )
            
        # fb_name_mapping = {
        #     'Trygghansa': 'Trygg-Hansa',
        #     'Mjoback': 'Mjöbäcks Pastorat Hästförsäkring',
        #     'Manypets': 'Many Pets',
        #     'Moderna': 'Moderna Försäkringar',
        #     'Dina': 'Dina Försäkringar',
        #     'Ica': 'ICA Försäkring'
        # }
        
        mask_fb = df['source'] == 'Insurance_Company'
        df.loc[mask_fb, 'originSender'] = df.loc[mask_fb, 'originSender'].replace(fb_name_mapping)
        
        specific_emails = ['djurskador@djurskador.se', 'wisentic']
        mask_specific = (df['source'] == 'Insurance_Company') & (df['parsedFrom'].isin(specific_emails))
        df.loc[mask_specific, 'originSender'] = 'Wisentic'
        return df
        
    def set_sender_clinic(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df['source'] == 'Other')
        
        if mask.any() and not self.clinic_list.empty:
            clinic_data = pd.merge(
                df.loc[mask], self.clinic_list[['clinicName', 'clinicEmail']], 
                left_on='parsedFrom', right_on='clinicEmail', how='left'
            )
            df.loc[mask, 'originSender'] = clinic_data['clinicName'].values
        
        missing = df['originSender'].isna()
        df.loc[missing, 'originSender'] = (df.loc[missing].apply(
            lambda row: expand_matching_clinic(row['parsedFrom'], self.clinic_keyword), axis=1)
        )
        
        # Handle Provet Cloud specific case
        mask_provet = (df['originSender'].isna() & (df['parsedFrom'].str.lower().str.contains('mailer.provet.email', na=False)))
        if mask_provet.any():
            df.loc[mask_provet, 'originSender'] = 'Provet_Cloud'
        
        # Set source to Clinic for matched entries (excluding Insurance_Company)
        mask_clinic_matched = (df['originSender'].notna() & (df['source'] != 'Insurance_Company'))
        if mask_clinic_matched.any():
            df.loc[mask_clinic_matched, 'source'] = 'Clinic'
        
        return df
    
    def set_sender_drp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Set sender for DRP (Data Recovery Partner)"""
        mask = ((df['source'] == 'Other') & (df['parsedFrom'].str.lower().str.contains(self.drp_str, na=False)))
        if mask.any():
            df.loc[mask, 'source'] = 'DRP'
            df.loc[mask & (df['originSender'].isna()), 'originSender'] = 'DRP'
        return df
    
    def set_sender_finance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Set sender for Finance"""
        mask = ((df['source'] == 'Other') & (df['parsedFrom'].str.lower().str.contains('fortus|payex', na=False)))
        if mask.any():
            df.loc[mask, 'source'] = 'Finance'

        mask_finance = (df['source'] == 'Finance')
        if mask_finance.any():
            df.loc[mask_finance, 'originSender'] = (df.loc[mask_finance, 'parsedFrom'].apply(
                lambda x: next((item.capitalize() for item in ['fortus', 'payex'] if isinstance(x, str) and item in x), None))
            )
        
        return df
    
    def set_sender_postmark(self, df: pd.DataFrame) -> pd.DataFrame:
        """Set sender for Postmark (remains as Other source)"""
        mask = ((df['source'] == 'Other') & df['originSender'].isna() 
                & (df['parsedFrom'].str.contains('@postmarkapp', na=False)))
        if mask.any():
            df.loc[mask, 'originSender'] = 'Postmark'
        return df


class ReceiverResolver(BaseService):
    def detect_receiver(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['parsedTo'] = df['to'].apply(parse_email_address)
        df[['clinicCompType', 'captured_fb']] = df.apply(
            lambda row: pd.Series(self._check_forward_part(row['source'], row['origin'])), axis=1
        )
        
        # Explode parsedTo to handle multiple receivers
        exploded = df.explode('parsedTo')
        exploded['sendTo'] = 'Other'
        exploded['originReceiver'] = None
        exploded['reference'] = None
        exploded['category'] = None
        exploded['errandId'] = exploded.apply(lambda _: [], axis=1)
        
        # Apply receiver detection methods in sequence
        exploded = self.set_receiver_clinic(exploded)
        exploded = self.set_receiver_special_add(exploded)
        exploded = self.set_receiver_fb(exploded)
        exploded = self.set_receiver_drp(exploded)
        exploded = self.set_receiver_finance(exploded)

        grouped_df = self._group(exploded)
        result_df = self._finalize(grouped_df)
        
        # Merge back with original df to preserve all columns
        original_columns = [col for col in df.columns if col not in result_df.columns or col == 'id']
        df_original = df[original_columns]
        final_df = pd.merge(df_original, result_df, on='id', how='left')
        final_df['receiver'] = final_df['originReceiver']

        return final_df
       
    def _check_forward_part(self, source: str, origin: str) -> Tuple[Optional[str], Optional[str]]:
        """Check forward part logic - placeholder for actual implementation"""
        fw_patts = [reg.compile(r, reg.DOTALL) for r in self.forward_words]
        fw_add_regs = self.forward_suggestion[self.forward_suggestion['action']=='Forward_Address'].templates.to_list()
        flag_de_sa = captured_fb = None

        for patt in fw_patts:
            if patt.search(origin):
                flag_de_sa = base_match(origin, self.clinic_complete_type)
                if source == 'Clinic':
                    matches = []
                    for tmpl in fw_add_regs:
                        for m in reg.findall(tmpl, origin):
                            if '@' in m and any(ic in m for ic in self.fb_ref_list):
                                matches.append(m)
                    if matches:
                        fw_add = base_match(matches[0], [r'(.*)']) or matches[0]
                        if fw_add.lower() != 'mail@direktregleringsportalen.se':
                            for ic in self.fb_ref_list:
                                if (ic == 'if' and ('if.' in fw_add or reg.search(r'\bif@', fw_add))) or (ic in fw_add):
                                    captured_fb = ic.capitalize()
                                    if ic in ['wisentic','djurskador@djurskador.se']:
                                        captured_fb = 'Wisentic'
                                    break
                break
        return flag_de_sa, captured_fb
    
    def set_receiver_clinic(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Set receiver for Clinic"""
        if not self.clinic_list.empty:
            exploded = pd.merge(
                exploded, self.clinic_list[['clinicName', 'clinicEmail']], 
                left_on='parsedTo', right_on='clinicEmail', how='left'
            )
        else:
            exploded['clinicName'] = None
        
        # Expand matching for unmatched entries using keywords
        mask = exploded['clinicName'].isna()
        if mask.any():
            exploded.loc[mask, 'clinicName'] = (exploded.loc[mask].apply(
                lambda row: expand_matching_clinic(row['parsedTo'], self.clinic_keyword), axis=1)
            )
        
        # Set originReceiver and sendTo
        exploded['originReceiver'] = exploded['clinicName']
        exploded = exploded.drop('clinicName', axis=1)
        
        # Set sendTo to Clinic for matched entries (excluding source=Clinic)
        mask_clinic = (exploded['originReceiver'].notna()) & (exploded['source'] != 'Clinic')
        exploded.loc[mask_clinic, 'sendTo'] = 'Clinic'
        
        return exploded
    
    def _special_email_add(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle special email addresses - placeholder for actual implementation"""
        df['reference'] = df['parsedTo'].str.extract(r'mail\+(\d+)@drp\.se')[0]
        ref_list = df['reference'].dropna().unique().tolist()
        ref_list = ",".join(f"'{ref}'" for ref in ref_list)
        ref_errand = fetchFromDB(self.errand_info_query.format(COND=f"ic.reference IN ({ref_list})"))
        df = pd.merge(df[['id','reference']], ref_errand, on='reference', how='left')

        return df[['clinicName','insuranceCompany', 'reference', 'errandId']]
    
    def set_receiver_special_add(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Set receiver for Insurance Company - Special Address"""
        # Handle special DRP addresses (mail+number@drp.se)
        mask = (exploded['sendTo'] == 'Other') & (
            exploded['parsedTo'].str.lower().str.contains(r'mail\+\d+@drp\.se', na=False, regex=True))
        if mask.any():
            exploded.loc[mask, ['source', 'sendTo', 'category']] = ['Clinic', 'Insurance_Company', 'Complement_DR_Clinic']
            special_subset = exploded[mask].reset_index(drop=True)
            special_results = self._special_email_add(special_subset)
            exploded.loc[mask, ['originSender', 'originReceiver', 'reference']] = (
                special_results[['clinicName', 'insuranceCompany', 'reference']].values
            )
            exploded.loc[mask, 'errandId'] = special_results['errandId'].apply(lambda x: [] if pd.isna(x) else [int(x)]).values

        return exploded
    
    def set_receiver_fb(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Set receiver for Insurance Company - Normal"""
        # Handle normal IC receivers from clinic
        mask = (exploded['source'] == 'Clinic') & (exploded['sendTo'] == 'Other')
        if mask.any():
            exploded.loc[mask, 'originReceiver'] = exploded.loc[mask, 'captured_fb']
            exploded.loc[mask, 'sendTo'] = 'Insurance_Company'
        
        # Standardize insurance company names
        mask_clinic_to_ic = (exploded['source'] == 'Clinic') & (exploded['sendTo'] == 'Insurance_Company')
        if mask_clinic_to_ic.any():
            exploded.loc[mask_clinic_to_ic, 'originReceiver'] = (
                exploded.loc[mask_clinic_to_ic, 'originReceiver'].replace(fb_name_mapping)
            )
        
        return exploded
    
    def set_receiver_drp(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Set receiver for DRP"""
        mask = (exploded['sendTo'] == 'Other') & (exploded['parsedTo'].str.lower().str.contains(self.drp_str, na=False))
        if mask.any():
            exploded.loc[mask, ['originReceiver', 'sendTo']] = ['DRP', 'DRP']
        
        return exploded
    
    def set_receiver_finance(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Set receiver for Finance"""
        mask = (exploded['sendTo'] == 'Other') & (exploded['parsedTo'].str.lower().str.contains(r'fortus|payex', na=False))
        if mask.any():
            exploded.loc[mask, 'originReceiver'] = (exploded.loc[mask, 'parsedTo'].apply(
                lambda x: next((item.capitalize() for item in ['fortus', 'payex'] if isinstance(x, str) and item in x.lower()), None))
            )
            exploded.loc[mask, 'sendTo'] = 'Finance'
        
        return exploded
    
    def _aggregate(self, filtered_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate exploded results back to original format"""
        subset_cols = ['sendTo', 'originReceiver', 'reference', 'errandId', 'category']
        agg_dict = {}
        for col in subset_cols:
            if col == 'errandId':
                agg_dict[col] = lambda x: list(set(item for sublist in x 
                    for item in (sublist if isinstance(sublist, list) else [sublist])
                ))
            else:
                agg_dict[col] = lambda x: list(set(x))
        
        grouped = filtered_df.groupby('id').agg(agg_dict).reset_index()
        
        return grouped
    
    def _group(self, exploded: pd.DataFrame) -> pd.DataFrame:
        """Helper function to group and aggregate exploded data back to original structure"""
        subset_cols = ['sendTo','originReceiver','reference', 'errandId', 'category']
        subset = [col for col in subset_cols if col in exploded.columns]
        
        if not subset:
            return exploded[['id']].drop_duplicates().reset_index(drop=True)
            
        filtered = exploded.dropna(how='all', subset=subset)
        
        if not filtered.empty:
            grouped = self._aggregate(filtered)
            return grouped
        else:
            # Return empty dataframe with expected columns
            result = exploded[['id']].drop_duplicates().reset_index(drop=True)
            for col in subset_cols:
                result[col] = None
            return result

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        subset_cols = ['sendTo', 'originReceiver', 'reference', 'category']
        for col in subset_cols:
            df[col] = df[col].apply(lambda x: [item for item in x if item is not None] if isinstance(x, list) else [])
            if col == 'sendTo':
                df[col] = df[col].apply(
                    lambda x: ','.join([item for item in x if item != 'Other'])
                    if isinstance(x, list) and any(item != 'Other' for item in x)
                    else 'Other' if isinstance(x, list) else x
                )
            else:
                df[col] = df[col].apply(lambda x: ','.join(x) if isinstance(x, list) and x else None)
        
        return df


class StaffResolver(BaseService):
    """Resolve staff animal information from email content using chain pattern"""
    def __init__(self):
        super().__init__()
        self.staff_animal_df = get_staffAnimal()
        # print(self.staff_animal_df.loc[self.staff_animal_df['Klinik']=='Veterinärkliniken Arken Zoo Nyköping'])
    
    def detect_staff_animals(self, df: pd.DataFrame) -> pd.DataFrame:
        needed = ['id','category','receiver','animalName','ownerName','subject','origin','email','isStaffAnimal']
        work = df[needed].copy()

        work['receiver']  = self._clean_staff_text(work['receiver'])
        work['receiver']  = self._normalize_staff_halsinge(work['receiver'])
        for col in ['animalName','ownerName','subject','origin']:
            work[col] = self._clean_staff_text(work[col])

        staff = self.staff_animal_df.copy()
        for col in staff.select_dtypes(include=['object','string']).columns:
            staff[col] = self._clean_staff_text(staff[col])
        staff['klinik_key']   = self._normalize_name(staff['Klinik'])
        staff['personal_key'] = self._normalize_name(staff['Personal'])
        staff['djur_key']     = self._normalize_name(staff['Djur'])

        work['clinic_key'] = self._normalize_name(work['receiver'])
        work['owner_key']  = self._normalize_name(work['ownerName'])
        work['animal_key'] = self._normalize_name(work['animalName'])

        ignore = {'Information','Auto_Reply','Finance_Report','Wisentic_Error','Insurance_Validation_Error'}
        work['to_check'] = ~work['category'].isin(ignore)

        merged = work.loc[work['to_check']].merge(
            staff[['klinik_key','personal_key','djur_key']],
            left_on='clinic_key', right_on='klinik_key', how='left'
        )

        owner_eq  = (merged['owner_key']  != '') & (merged['owner_key']  == merged['personal_key'])
        animal_eq = (merged['animal_key'] != '') & (merged['animal_key'] == merged['djur_key'])

        cond_full        = owner_eq & animal_eq
        cond_only_animal = animal_eq & (merged['personal_key'] == '')
        cond_only_owner  = owner_eq  & (merged['djur_key']     == '')
        merged['hit']    = cond_full | cond_only_animal | cond_only_owner
        
        hit_ids = set(merged.loc[merged['hit'], 'id'].unique())
        special_mask = (
            work['to_check'] &
            (~work['id'].isin(hit_ids)) &
            ( work['subject'].str.contains('omatchad utbetalning', na=False) |
              work['origin'].str.contains('omatchad utbetalning', na=False) )
        )

        staff_field_dict = self._build_staff_dict(staff[['Klinik','Personal','Djur']])
        special_ids = []
        for _, r in work.loc[special_mask, ['id','email']].iterrows():
            parts = (r['email'] or '').split('[body]')
            body = parts[1] if len(parts) > 1 else parts[0]
            if self._match_field_dict_without_order(body, staff_field_dict):
                special_ids.append(r['id'])

        out = df.copy()
        if hit_ids:
            out.loc[out['id'].isin(hit_ids), 'isStaffAnimal'] = True
        if special_ids:
            out.loc[out['id'].isin(special_ids), 'isStaffAnimal'] = True
        
        return out
    
    @staticmethod
    def _clean_staff_text(s: pd.Series) -> pd.Series:
        return (s.astype("string")
             .str.replace(r'[,-]', ' ', regex=True)
             .str.replace('kliniken', 'klinik', regex=False)
             .str.replace(r'\(.*?\)', '', regex=True)
             .str.replace(r'\s+', ' ', regex=True)
             .str.strip()
             .str.lower())
    
    @staticmethod
    def _normalize_name(s: pd.Series) -> pd.Series:
        def tokkey(x: str) -> str:
            if not isinstance(x, str) or not x:
                return ''
            toks = lower_and_split(x)
            return ' '.join(sorted(toks)) if toks else ''
        return s.fillna('').map(tokkey)
    
    @staticmethod
    def _normalize_staff_halsinge(s: pd.Series) -> pd.Series:
        repl = {
            'hälsinge smådjursklinik hudiksvall': 'hälsingevet',
            'hälsinge smådjursklinik söderhamn': 'hälsingevet',
            'hälsinge smådjursklinik ljusdal': 'hälsingevet',
        }
        return s.map(lambda x: repl.get(x, x))
    
    @staticmethod
    def _build_staff_dict(staff_animal_df: pd.DataFrame) -> dict: 
        from collections import defaultdict
        d = {'Klinik': defaultdict(set), 'Personal': defaultdict(set), 'Djur': defaultdict(set)}
        for col in ['Klinik', 'Personal', 'Djur']:
            for text in staff_animal_df[col].dropna().unique():
                words = text.split()
                if words:
                    d[col][len(words)].add(frozenset(words))
        return d

    @staticmethod
    def _match_field_dict_without_order(email_text: str, staff_field_dict: dict) -> bool:
        cleaned = reg.sub(r'[^\w\såäöÅÄÖ\[\]\n]', '', (email_text or '').lower())
        words = cleaned.split()
        res = {'match_clinic': False, 'match_staff': False, 'match_animal': False}
        for length in range(1, len(words) + 1):
            ws_list = [frozenset(words[i:i+length]) for i in range(len(words) - length + 1)]
            for ws in ws_list:
                if ws in staff_field_dict['Klinik'].get(length, set()):
                    res['match_clinic'] = True
                if ws in staff_field_dict['Personal'].get(length, set()):
                    res['match_staff'] = True
                if ws in staff_field_dict['Djur'].get(length, set()):
                    res['match_animal'] = True
            if all(res.values()):
                return True
        return False


class AddressResolver(BaseService):
    """Address resolver service for email forwarding using chain pattern"""
    
    def __init__(self):
        super().__init__()
        self._setup_resolver_configs()
        self._setup_resolver_queries()
        self.result = {
            'forwardAddress': '',
            'adminInfo': ''
        }
    
    def _setup_resolver_queries(self):
        """Setup resolver-specific queries from the queries DataFrame"""
        try:
            # Extract resolver-specific queries if they exist
            if hasattr(self, 'queries'):
                pass  # Add specific query extractions if needed
        except Exception as e:
            print(f"Failed to setup resolver queries: {str(e)}")
    
    def _setup_resolver_configs(self):
        try:
            self.fb_forw_add = self.fb[:17].set_index('insuranceCompany')['forwardAddress'].to_dict()
            self.clinic_forw_add = self.clinic_list.loc[
                self.clinic_list['role'] == 'main_email', ['clinicName','clinicEmail']
            ].drop_duplicates()
            
        except Exception as e:
            print(f"Failed to setup resolver configs: {str(e)}")
            self.fb_forw_add = {}
            self.clinic_forw_add = pd.DataFrame()
  
    def detect_forward_address(self, source: str, receiver: str, user_id: Optional[int] = None) -> str:
        """Chain all address resolution methods"""
        # Reset result for new detection
        self.result = {
            'forwardAddress': '',
            'adminInfo': ''
        }
        
        # Chain resolution methods
        result = (self._resolve_clinic_forward(source, receiver)
                     ._resolve_fb_forward(source, receiver)
                     .resolve_admin_details(user_id)
                     .result)

        return result['forwardAddress']
    
    def _resolve_clinic_forward(self, source: str, receiver: str):
        """Resolve forwarding for clinic emails"""
        if self.result['forwardAddress']:  
            return self
        
        if source == 'Clinic':
            forward_addr = self.fb_forw_add.get(receiver, "")
            if forward_addr:
                self.result.update({
                    'forwardAddress': forward_addr,
                    'isValid': True
                })
        
        return self
    
    def _resolve_fb_forward(self, source: str, receiver: str):
        """Resolve forwarding for insurance company emails"""
        if self.result['forwardAddress']:  
            return self
        
        if source == 'Insurance_Company':
            clinic_match = self.clinic_forw_add[self.clinic_forw_add['clinicName'] == receiver]
            if not clinic_match.empty:
                forward_addr = clinic_match.iloc[0]['clinicEmail']
                self.result.update({
                    'forwardAddress': forward_addr,
                    'isValid': True
                })
        
        return self
    
    def resolve_admin_details(self, user_id: Optional[int]):
        """Resolve admin user information"""
        if not user_id:
            return self
        
        try:
            admin_df = fetchFromDB(self.admin_query.format(COND=f"id = {user_id}"))
            admin_name = admin_df['firstName'].values[0] if not admin_df.empty else ''
            self.result['adminInfo'] = admin_name
        except Exception as e:
            print(f"Failed to resolve admin info: {str(e)}")
        
        return self
    



