import regex as reg
import pandas as pd
from .base_service import BaseService
from .utils import fetchFromDB
from typing import Optional

class Classifier(BaseService):    
    def __init__(self):
        super().__init__()
        self.info_query = self.queries['info'].iloc[0]
        self.category_reg_list = pd.read_csv(f"{self.folder}/categoryReg.csv")
        self.category_list = self.category_reg_list['category'].unique().tolist()
        self.category_patterns = {category: reg.compile('|'.join(f"(?:{p})" for p in regs.dropna() if str(p)), reg.IGNORECASE)
                                    for category, regs in self.category_reg_list.groupby('category')['regex']
                                    if not regs.dropna().empty
                                }
    
    def initialize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        number_cols = self.number_reg_list['number'].unique()
        amount_cols = {'settlementAmount', 'attach_settlementAmount','totalAmount', 'attach_totalAmount','folksamOtherAmount'}
        name_cols = {'animalName', 'attach_animalName', 'animalName_Sveland','ownerName', 'attach_ownerName', 'insuranceCompanyReference'}
        id_string_cols = {'reference', 'insuranceNumber', 'damageNumber','insuranceCaseRef'}
        int_cols = set()
        default_values = {
            'isStaffAnimal': False,
            'showPage': 'Koppla',
            'note': 'Unreliable',
            'errandDate': pd.NaT,
            'category': None,
            'errandId': [[] for _ in range(len(df))],
            'paymentOption': None,
            'strategyType': None,
            'connectedCol': None,
            'errand_matched': False,
        }
        for col in amount_cols:
            default_values[col] = pd.NA
        for col in name_cols | id_string_cols:
            default_values[col] = pd.NA
        for col in int_cols:
            default_values[col] = pd.NA
        for col in number_cols:
            if col not in default_values:
                default_values[col] = pd.NA
        for col, value in default_values.items():
            if col not in df.columns:
                df[col] = value
            elif col == 'note' and df[col].isna().all():
                # Only set default note if all values are null
                df[col] = value

        # convert dtype
        for col in amount_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        for col in name_cols | id_string_cols:
            df[col] = df[col].astype('string')

        df['errandDate'] = pd.to_datetime(df['errandDate'], errors='coerce', utc=True).dt.tz_convert('Europe/Stockholm')

        return df
    
    def process_fbReference(self, df: pd.DataFrame) -> pd.DataFrame:
        fb_reference = self.fb[['insuranceCompany','insuranceCompanyReference']][:-7]
        df = pd.merge(df, fb_reference, left_on='sender', right_on='insuranceCompany', how='left').drop('insuranceCompany', axis=1)
        return df
    
    def identify_first_info(self, infoDate, subject: str) -> str:
        safe_subject = subject.replace("'", "''")
        condition = (f"""ecr."timestamp" >= ('{infoDate}'::timestamptz) - INTERVAL '5 minutes'
                        AND ecr."timestamp" < '{infoDate}'::timestamptz
                        AND e.subject = '{safe_subject}'""" )
        isFirst = fetchFromDB(self.info_query.format(CONDITION=condition))
        isFirst['timestamp'] = pd.to_datetime(isFirst['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
    
        if isFirst.empty:
            return 'FirstInfo'
        return 'Discard'
    
    def process_info(self, df: pd.DataFrame) -> pd.DataFrame:
        info_mask = df['category'] == 'Information'
        if info_mask.any():
            df.loc[info_mask, 'note'] = df.loc[info_mask, ['date', 'subject']].apply(lambda row: self.identify_first_info(row['date'], row['subject']), axis=1)
        return df
    
    def enrich_staff_animal(self, df: pd.DataFrame) -> pd.DataFrame:
        from .resolver import StaffResolver
        staff_resolver = StaffResolver()
        return staff_resolver.detect_staff_animals(df)
    
    def process_show_page(self, df: pd.DataFrame) -> pd.DataFrame:
        settle_cate_mask = df['category'].isin(['Settlement_Approved', 'Settlement_Denied'])
        sender_mask = ~df['sender'].isin(['Sveland', 'Dina Försäkringar'])
        reliable_note_mask = (df['note'] == 'Reliable')
        
        df.loc[settle_cate_mask & sender_mask, 'showPage'] = 'Ersätt'
        df.loc[~settle_cate_mask & reliable_note_mask, 'showPage'] = 'Mail'
        return df
    
    def get_ic_ref(self, errandId: int) -> Optional[str]:
        errandInfo = fetchFromDB(self.errand_info_query.format(COND=f"er.id = {errandId}"))
        if not errandInfo.empty:
            return errandInfo['reference'].iloc[0]

        return None

    def process_icReference(self, df: pd.DataFrame) -> pd.DataFrame:
        mask_ic_ref = (df['insuranceCaseRef'].isna() & (df['errandId'].apply(lambda x: len(x) == 1 if isinstance(x, (list, tuple)) else bool(x))))
        if mask_ic_ref.any():
            df.loc[mask_ic_ref, 'insuranceCaseRef'] = (df.loc[mask_ic_ref, 'errandId'].apply(lambda x: self.get_ic_ref(x[0] if isinstance(x, (list, tuple)) else x)))
        return df
    
    def refine_finalize(self, df: pd.DataFrame) -> pd.DataFrame:    
        out = (df.pipe(self.refine_categories)
                 .pipe(self.process_fbReference)
                 .pipe(self.process_info)
                 .pipe(self.enrich_staff_animal)
                 .pipe(self.process_show_page)
                 .pipe(self.process_icReference)
            )
        final_cols = [
            'id','from','sender','source','to','receiver',
            'insuranceCompanyReference','category','errandId',
            'totalAmount','settlementAmount','reference',
            'insuranceCaseRef','insuranceNumber','damageNumber',
            'animalName','ownerName','note','connectedCol','showPage','isStaffAnimal']
        out = out[[c for c in final_cols if c in out.columns]]
        return out
            
    def categorize_emails(self, df: pd.DataFrame) -> pd.DataFrame:
        """categorize emails based on predefined patterns and rules."""
         
        category_rules = [
            {'category': 'Complement_Reply', 'mask': (df['source'] != 'Insurance_Company')},
            {'category': 'Complement', 'mask': (df['source'] != 'Clinic')},
            {'category': 'Settlement_Request', 'mask': (df['source'] != 'Clinic')},
            {'category': 'Insurance_Validation_Error', 'mask': (df['source'] != 'Clinic')},
            {'category': 'Wisentic_Error', 'mask': (df['originSender'] == 'Wisentic')},
            {'category': 'Settlement_Approved', 'mask': (df['source'] == 'Insurance_Company')},
            {'category': 'Message', 'mask': True}, 
        ]

        unassigned_mask = df['category'].isna()

        for rule in category_rules:
            category = rule['category']
            if category not in self.category_patterns:
                continue
            pattern = self.category_patterns.get(category)

            text = df['email'].astype(str)
            mask_regex = text.map(lambda s: bool(pattern.search(s)))
            combined_mask = unassigned_mask & rule['mask'] & mask_regex.fillna(False)
            
            df.loc[combined_mask, 'category'] = category
            unassigned_mask = df['category'].isna() 

        msg_mask = (df['category'].isna()) & \
                   (df['settlementAmount'].isna()) & (df['totalAmount'] >= 0)
        df.loc[msg_mask, 'category'] = 'Message'
        
        remaining_categories = [
            cat for cat in self.category_list if cat not in {r['category'] for r in category_rules}
        ]
        unassigned_mask = df['category'].isna()
        for category in remaining_categories:
            if category in self.category_patterns:
                pattern = self.category_patterns[category]
                text = df['email'].astype(str)
                mask_regex = text.map(lambda s: bool(pattern.search(s)))
                mask = unassigned_mask & mask_regex.fillna(False)
                df.loc[mask, 'category'] = category
                unassigned_mask = df['category'].isna()
        
        return df

    def refine_categories(self, df: pd.DataFrame) -> pd.DataFrame:
     # refine Settlement_Denied
        mask_denied_approved = (df['category'] == 'Settlement_Denied') & (df['settlementAmount'] == 0)
        df.loc[mask_denied_approved, 'category'] = 'Settlement_Approved'
        
        mask_denied_set_zero = (df['category'] == 'Settlement_Denied') & (df['settlementAmount'].isna())
        df.loc[mask_denied_set_zero, 'settlementAmount'] = 0

     # process Folksam new format
        mask_folksam = (df['category'] == 'Settlement_Approved') & \
                       (df['sender'] == 'Folksam') & \
                       (df['folksamOtherAmount'].notna()) & \
                       (df['folksamOtherAmount'] != 0)
        if mask_folksam.any():
            df.loc[mask_folksam, 'totalAmount'] += df.loc[mask_folksam, 'folksamOtherAmount']

     # distingish DR and SÄ
       # for FB
        is_complement = df['category'] == 'Complement'
        is_dr_insurance = (df['paymentOption'].notna()) | \
                          (df['strategyType'].isin(['settlement', 'creditcheck', 'reservation', 'parallel'])) | \
                          (df['errandId'].apply(lambda x: len(x) > 0 if isinstance(x, (list, tuple)) else bool(x)))
        is_damage_req_insurance = (df['paymentOption'].isna()) | \
                                  (df['strategyType'].isna()) | \
                                  (df['strategyType'] == 'virtual') | \
                                  (df['errandId'].apply(lambda x: len(x) == 0 if isinstance(x, (list, tuple)) else not bool(x)))

        df.loc[is_complement & is_dr_insurance, 'category'] = 'Complement_DR_Insurance_Company'
        df.loc[is_complement & is_damage_req_insurance, 'category'] = 'Complement_Damage_Request_Insurance_Company'

       # for Clinic
        is_clinic = (df['source'] == 'Clinic')
        df.loc[is_clinic & (df['clinicCompType'] == 'skadeanmälan'), 'category'] = 'Complement_Damage_Request_Clinic'
        df.loc[is_clinic & (df['clinicCompType'] == 'direktreglering'), 'category'] = 'Complement_DR_Clinic'
        
       # further correcting
        has_errand = df['errandId'].apply(lambda x: len(x) > 0 if isinstance(x, (list, tuple)) else bool(x))
        no_errand = ~has_errand
        
        df.loc[df['category'].isin(['Complement_Reply', 'Complement_Damage_Request_Clinic']) & has_errand, 'category'] = 'Complement_DR_Clinic'
        df.loc[df['category'].isin(['Complement_Reply', 'Complement_DR_Clinic']) & no_errand, 'category'] = 'Complement_Damage_Request_Clinic'

        # mark the rest as "Manual_Handling_Required"
        df['category'] = df['category'].fillna('Manual_Handling_Required')
        
        return df

    def statistic(self, df):
        all = df.shape[0]
        connected = df[df['errandId'].apply(len) > 0].shape[0]
        singleConnect = df[df['errandId'].apply(len) == 1].shape[0]
        note_values = df['note'].value_counts()
        # print(f"Debug: Note column values: {note_values.to_dict()}")
        reliable = df[df['note']=='Reliable'].shape[0]
        # print(f"Debug: Reliable count: {reliable}")
        auto = df[df['category']!='Manual_Handling_Required'].shape[0]
        manu = df[df['category']=='Manual_Handling_Required'].shape[0]
        staff_animals = df[df['isStaffAnimal']==True].shape[0]
        print(f'''all_records: {all}, auto_categorized: {auto}, {auto/all*100:.2f}%, manual: {manu}, {manu/all*100:.2f}%, 
                  connected_to_errands: {connected}, {connected/all*100:.2f}%, single_errandId: {singleConnect}, {singleConnect/all*100:.2f}%, \
                  reliable_connection: {reliable}, {reliable/all*100:.2f}%, staff_animal: {staff_animals}, {staff_animals/all*100:.2f}%''')
        
        # Return statistics as dictionary for template display
        stats_dict = {
            'total_records': all,
            'auto_categorized': f"{auto} ({auto/all*100:.1f}%)",
            'manual_handling_required': f"{manu} ({manu/all*100:.1f}%)",
            'connected_to_errands': f"{connected} ({connected/all*100:.1f}%)",
            'single_errand_connection': f"{singleConnect} ({singleConnect/all*100:.1f}%)",
            'reliable_connections': f"{reliable} ({reliable/all*100:.1f}%)",
            'staff_animals': f"{staff_animals} ({staff_animals/all*100:.1f}%)"
        }

        categoryList = [item for item in self.category_list if item not in ['Complement', 'Complement_Reply']]
        categoryList += ['Complement_DR_Insurance_Company','Complement_Damage_Request_Insurance_Company','Complement_DR_Clinic','Complement_Damage_Request_Clinic']

        for category in categoryList:
            mask_connect = (df['errandId'].apply(lambda x: len(x) > 0 if isinstance(x, (list, tuple)) else bool(x)))
            mask_unconnect = (df['errandId'].apply(lambda x: len(x) == 0 if isinstance(x, (list, tuple)) else not bool(x)))
            if (category != 'Complement') and (category != 'Complement_Reply'):
                mask_subCate = (df['category']==category)
                print(f" - {category}: {df[mask_subCate].shape[0]}, connect:{df[mask_subCate & mask_connect].shape[0]}, un-connect: {df[mask_subCate & mask_unconnect].shape[0]}")
            elif category == 'Complement':
                mask_dr = (df['category'] == 'Complement_DR_Insurance_Company')
                mask_sa = (df['category'] == 'Complement_Damage_Request_Insurance_Company')
                print(f" - Complement_DR_Insurance_Company: {df[mask_dr].shape[0]}, connect:{df[mask_dr & mask_connect].shape[0]}, un-connect: {df[mask_dr & mask_unconnect].shape[0]}")
                print(f" - Complement_Damage_Request_Insurance_Company:,df[mask_sa].shape[0], connect:{df[mask_sa & mask_connect].shape[0]}, un-connect: {df[mask_sa & mask_unconnect].shape[0]}")
            else:
                mask_dr = (df['category'] == 'Complement_Damage_Request_Clinic')
                mask_sa = (df['category'] == 'Complement_DR_Clinic')
                print(f" - Complement_Damage_Request_Clinic:, df.loc[mask_df].shape[0], connect:{df[mask_dr & mask_connect].shape[0]}, un-connect: {df[mask_dr & mask_unconnect].shape[0]}")
                print(f" - Complement_DR_Clinic, df.loc[mask_df].shape[0], connect:{df[mask_sa & mask_connect].shape[0]}, un-connect: {df[mask_sa & mask_unconnect].shape[0]}")
    
                # other statistic   
                # print("0 kr",df[(df['settlementAmount'].notna()) & (df['settlementAmount']==0)].shape[0])
                # print(">0 kr",df[(df['settlementAmount'].notna()) & (df['settlementAmount']>0)].shape[0]) 
                # df.loc[df['category']==category,['id','originSender','email']].sort_values(by='originSender').to_csv(f"data/results/{category}.csv", index=False)

                # sub = df.loc[df['category']==category]
                # sub_all = sub.shape[0]
                # sub_connected = sub[sub['errandId'].apply(len) > 0].shape[0]
                # print(f"sub_connected: {sub_connected}, {sub_connected/sub_all*100:.2f}% {sub.groupby('connectedCol').shape[0]}")
                # for col in self.number_reg_list.number.drop_duplicates().to_list():
                #     if col != 'animalName_Sveland' and (df.loc[(df[col].notna())].shape[0] != 0):
                #         print(f" ** ** {col}: {df.loc[(df[col].notna())].shape[0]} non-null, {(df.loc[df[col].notna()].shape[0])/all*100:.02f}%")
            
            # print("Extracted numbers and names:")
            # for col in self.number_reg_list.number.drop_duplicates().to_list():
            #     if col != 'animalName_Sveland':
            #         print(f" - {col}: {df.loc[df[col].notna()].shape[0]} non-null, {(df.loc[df[col].notna()].shape[0])/all*100:.02f}%")
        
        return stats_dict
    