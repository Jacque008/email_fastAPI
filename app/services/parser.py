import regex as reg
import pandas as pd
from .utils import fetchFromDB, extract_first_address, base_match
from .extractor import Extractor
from .base_service import BaseService

class Parser(BaseService):  
    def __init__(self):
        super().__init__()
        self.extractor = Extractor()
        self.wisentic_sender_patts = {
            'If': [
                'if@djurskador.se',
                'djurskador@if.se',
                'If Skadeförsäkring',
                'If Djurskador',
                'If betalar er'
            ],
            'Sveland': [
                'Sveland betalar er',
                'kontakta oss på Sveland',
                '@sveland.se',
                'sveland@',
                'Sveland Djurförsäkringar'
            ],
            'Folksam': [
                'Exempel på korrekt format är: CV-1234567-123'
            ]
        }
    
    def parse_forward_part(self, source, text): # pass
        """
        Parse the sender of an email from a forwarded email, and check if it is Direktreglering or Skadeanmänlan.
        """
        fw_patts = [reg.compile(r, reg.DOTALL) for r in self.forward_words]
        fw_adds_reg_list = self.forward_suggestion[self.forward_suggestion['action']=='Forward_Address'].templates.to_list()
        flag_dr_sa = captured_fb = fw_adds = None

        for patt in fw_patts:
            if patt.search(text):
                flag_dr_sa = base_match(text, self.clinic_complete_type)
                if source == 'Clinic':
                    matches = []
                    for adds_reg in fw_adds_reg_list:
                        for m in reg.findall(adds_reg, text):
                            if '@' in m and any(fb in m for fb in self.fb_ref_list):
                                matches.append(m)
                    if matches:
                        fw_adds = extract_first_address(matches)
                        if fw_adds.lower() != 'mail@direktregleringsportalen.se':
                            for fb in self.fb_ref_list:
                                if (fb == 'if' and ('if.' in fw_adds or reg.search(r'\bif@', fw_adds) or 'If Skadeförsäkring' in fw_adds)) or fb in fw_adds: 
                                    captured_fb = fb.capitalize()
                                    
                                    if fb in ['wisentic','djurskador@djurskador.se']:
                                        captured_fb = 'Wisentic'
                                    break
                break
            
        return flag_dr_sa, captured_fb

    def parse_provet_cloud_row(self, text, errandIds): # pass
        """
        Parse sender and receiver of a completment reply email came from Provet Cloud.
        """
        email, sender, receiver = '', 'Provet_Cloud', None
        m = base_match(text, self.msg_provetcloud_reg)
        email = f"[BODY]{m}" if m else "[BODY]Provet_Cloud blank msg"

        if not errandIds:
            clinic = base_match(text, self.clinic_provetcloud_reg)
            if clinic:
                mClinic = self.extractor.extract_clinic_by_kws(clinic)
                if mClinic: sender = mClinic
            # receiver logic
            mapping = [
               (lambda t: 'If', lambda t: 'If Skadeförsäkring' in t or reg.search(r'KD\d*-\d*-?\d*', t)),
               (lambda t: 'Folksam', lambda t: reg.search(r'och FF\d+S|CV-?\d*-?\d*', t)),
               (lambda t: 'Lassie', lambda t: reg.search(r'VOFF-?\w*-?\d*', t)),
               (lambda t: 'Svedea', lambda t: reg.search(r'HU\d{2}-|försäkringsnummer \d+', t)),
               (lambda t: None, lambda t: base_match(t, self.receiver_provetcloud_reg) is not None),
            ]
            for func, cond in mapping:
                if cond(text):
                    receiver = func(text) or receiver
                    break
            if receiver and receiver.lower().find("many")>=0 and receiver.lower().find("pets")>=0:
                receiver = "Many Pets"
            
            for k, v in self.receiver_mappings.items():
                if not receiver and (k + ' ') in text.lower():
                    receiver = k
                if k in (receiver or "").lower(): 
                    receiver = v; break

        elif len(errandIds)==1:
            df = fetchFromDB(self.errand_info_query.format(COND=f"er.id = {errandIds[0]}"))
            if not df.empty:
                sender = df['clinicName'].iloc[0]
                receiver = df['insuranceCompany'].iloc[0]

        return pd.Series({'email': email, 'sender': sender, 'receiver': receiver})

    def handle_provet_cloud(self, df):
        df['sender'], df['receiver'] = df['originSender'], df['originReceiver']
        mask = (df['originSender'] == 'Provet_Cloud')
        if mask.any():
            df.loc[mask, ['email', 'sender', 'receiver']] = df.loc[mask, ['email', 'errandId']
                ].apply(lambda row: self.parse_provet_cloud_row(row['email'], row['errandId']), axis=1) 
                  
        return df
         
    def parse_wisentic_row(self, text):
        """
        Determine actual sender / receiver from Wisentic based on email content.
        """
        text_lower = text.lower()
        for sender, patterns in self.wisentic_sender_patts.items():
            for pattern in patterns:
                if pattern.lower() in text_lower:
                    return sender
        return 'Wisentic'

    def handle_wisentic(self, df):
        """
        Apply sender detection logic to entire DataFrame where originSender is Wisentic.
        """
        mask_sender = (df['originSender'] == 'Wisentic')
        if mask_sender.any():
            df.loc[mask_sender, 'sender'] = df.loc[mask_sender, 'email'].apply(self.parse_wisentic_row)

        mask_receiver = (df['originReceiver'] == 'Wisentic')
        if mask_receiver.any():
            df.loc[mask_receiver, 'receiver'] = df.loc[mask_receiver, 'email'].apply(self.parse_wisentic_row)

        return df
    
    def pasre_special_email_adds(self, df):
        df['reference'] = df['parsedTo'].str.extract(r'mail\+(\d+)@drp\.se')[0]

        refList = df['reference'].dropna().unique().tolist()
        refList = ",".join(f"'{ref}'" for ref in refList)
        refDB = fetchFromDB(self.errand_info_query.format(COND=f"ic.reference IN ({refList})"))
        df = pd.merge(df[['id','reference']], refDB, on='reference', how='left')

        return df[['clinicName','insuranceCompany', 'reference', 'errandId']]
    
    