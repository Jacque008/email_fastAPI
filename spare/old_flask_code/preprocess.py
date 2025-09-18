import os
import regex as reg
import pandas as pd
from .utils import fetchFromDB
from dotenv import load_dotenv
load_dotenv()

class PreProcess():
    def __init__(self): 
        if os.getenv('ENV_MODE') == 'local':
            self.folder = "data/para_tables" 
        if os.getenv('ENV_MODE') == 'test':
            self.folder = "gs://ml_email_test"
        elif os.getenv('ENV_MODE') == 'production':
            self.folder = "gs://ml_email_category"

        self.ic = pd.read_csv(f"{self.folder}/ic.csv") 
        self.icRefList = self.ic[:-5].insuranceCompanyReference.tolist() # 18 ICs + 2 Wisentic
        self.icRefStr = '|'.join([reg.escape(ic) for ic in self.icRefList]) # 17 ICs + 2 Wisentic
        self.drpStr = '|'.join([reg.escape(ic) for ic in self.ic[-5:].insuranceCompanyReference.tolist()]) # 5 Drp support emails
        self.clinic = pd.read_csv(f"{self.folder}/clinic.csv") 
        self.clinicList = self.clinic[['clinicId','clinicName', 'clinicEmail']].sort_values(['clinicId','clinicName']).drop_duplicates(subset=['clinicEmail'],keep='last')
        self.clinicKeyWords = self.clinic.loc[self.clinic['keyword'].notna(), ['clinicName','keyword']].drop_duplicates()
        self.clinicKeyWords['keyword'] = self.clinicKeyWords['keyword'].apply(lambda x: x.split(',') if isinstance(x, str) else [])   
        self.clinicPC = self.clinic.loc[self.clinic['provetCloud'].notna(), ['clinicName','provetCloud']].drop_duplicates()
        self.clinicPC['keyword'] = self.clinicPC['provetCloud'].apply(lambda x: x.split(',') if isinstance(x, str) else [])
        self.clinicCompType = pd.read_csv(f"{self.folder}/clinicCompType.csv").complement.tolist()
        
        self.stopWords = pd.read_csv(f"{self.folder}/stopWords.csv").stopWords.tolist()
        self.forwardWords = pd.read_csv(f"{self.folder}/forwardWords.csv").forwardWords.tolist()
        self.forwardSugg = pd.read_csv(f"{self.folder}/forwardSuggestion.csv")
        
        self.msgPCReg = self.forwardSugg[self.forwardSugg['action']=='ProvetCloud_Msg'].templates.to_list()
        self.clinicPCReg = self.forwardSugg[self.forwardSugg['action']=='ProvetCloud_Clinic'].templates.to_list() 
        self.recipientPCReg = self.forwardSugg[self.forwardSugg['action']=='ProvetCloud_Recipient'].templates.to_list()
        self.receiver_mappings = {
                'sveland': 'Sveland',
                'agria': 'Agria',
                'dina': 'Dina Försäkringar',
                'trygg': 'Trygg-Hansa',
                'moderna': 'Moderna Försäkringar',
                'ica': 'ICA Försäkring',
                'hedvig': 'Hedvig',
                'dunstan': 'Dunstan',
                'petson': 'Petson'} 
        
        forwCates = self.forwardSugg[self.forwardSugg['action'].str.endswith('_Template')].action.to_list()
        self.forwCates = [item.replace('_Template', '') for item in forwCates]
        
        self.queries = pd.read_csv(f"{self.folder}/queries.csv")
        self.emailSpecQuery = self.queries['emailSpec'].iloc[0] # keep for preProcess, createdforwarding and app.py
        # self.specialAddParseQuery = self.queries['specialAddParse'].iloc[0] # keep for preProcess, paymentMatching and app.py     
        self.adminQuery = self.queries['admin'].iloc[0] # keep for app.py login
        self.updateClinicEmailQuery = self.queries['updateClinicEmail'].iloc[0] # keep for app.py update clinic email list automatically daily
        self.errandInfoQuery = self.queries['errandInfo'].iloc[0] # keep for app.py for Complement_Reply email from Provet Cloud 
        
    def _cleanText(self, text):
        import html
        
        if text is None:
            return text
        
        text = html.unescape(text)
        if reg.search(r'</?\w+[^>]*>', text):
            text = reg.sub(r'</p\s*>', '</p>\n', text, flags=reg.IGNORECASE)
            text = reg.sub(r'<[^>]+>', '', text)
            
        special_chars = {
            '“': '"',
            '”': '"',
            '\xa0': ' ',
            '\u200b': '\n',
            '\ufeff': '\n',
            r'\r': '\n',
            r'\n>': '\n',
            r' \n': '\n',
            r'\n ': '\n',}
        for code, char in special_chars.items():
            text =reg.sub(code, char, text, flags=reg.MULTILINE)
        
        text = reg.sub(r'\n^[>]+ ?','\n', text, flags=reg.MULTILINE)
        text = reg.sub(r'_([^_]+)_', r'\1', text)    
        text = reg.sub(r'[ \t]+',' ', text)
        text = reg.sub(r'\n\n+','\n\n', text)
        
        return text.strip()

    def _getTextFromHtml(self, html):
        from inscriptis import get_text
        text = get_text(html)
        text_cleanTexted = self._cleanText(text)

        return text_cleanTexted
    
    def _baseMatch(self, text, regex_list):
        patterns = [reg.compile(item, reg.DOTALL|reg.MULTILINE ) for item in regex_list]
        for pattern in patterns:
            matched = pattern.search(text)
            if matched:
                # print(pattern)
                # print(matched)
                # print(matched.group(1))
                return matched.group(1)
            
        return None
 
    def _checkForwardPart(self, source, text):    
        flagDrSa, capturedIc, forwardAddress = None, None, None

        forwardWordsPatterns = [reg.compile(item, reg.DOTALL) for item in self.forwardWords]
        forwrdAddressReg = self.forwardSugg[self.forwardSugg['action']=='Forward_Address'].templates.to_list()

        for pattern in forwardWordsPatterns:
            isForward = reg.search(pattern, text)

            if isForward:
                flagDrSa = self._baseMatch(text, self.clinicCompType)
                if source == 'Clinic':  
                    fwAddList = []
                    for fwRegex in forwrdAddressReg:
                        matches = reg.findall(fwRegex, text)  # 使用单个正则表达式进行查找
                        if matches:  
                            for match in matches:
                                if '@' in match:
                                    if any(keyword in match for keyword in self.icRefList):
                                        fwAddList.append(match)
                    if fwAddList:
                        forwardAddress = self._parseEmailAddress(fwAddList)
                        forwardAddress = forwardAddress[0]
                        if forwardAddress != 'mail@direktregleringsportalen.se':
                            for ic in self.icRefList:
                                if ic == 'if':
                                    if ('if.' in forwardAddress) or reg.search(r'\bif@', forwardAddress) or 'If Skadeförsäkring' in text:
                                        capturedIc = ic.capitalize()
                                        break 
                                else:
                                    if ic in forwardAddress:
                                        capturedIc = ic.capitalize()
                                        if ic in ['wisentic','djurskador@djurskador.se']:
                                            capturedIc = 'Wisentic'
                                        break  
                break  

        return flagDrSa, capturedIc

    def _truncate(self, text, trunRegList):
        def findTruncWord(text, trunRegList):
            stopPos = len(text)
            patterns = [reg.compile(item, reg.DOTALL) for item in trunRegList]
            
            for pattern in patterns:
                matched = reg.search(pattern, text)
                if matched:
                    # print(pattern)
                    # print(matched)
                    if (0 <= matched.start() < stopPos):
                        stopPos = matched.start()
                        
            return stopPos

        subject = text.split('[BODY]', 1)[0]
        subjectLength = len(subject + '[BODY]')
        first_stopPos = findTruncWord(text, trunRegList)

        if first_stopPos > subjectLength:
            # print("1 first_stopPos > subjectLength:",text[:first_stopPos].strip())
            return text[:first_stopPos].strip()
        
        elif first_stopPos == subjectLength:
            next_stopPos = findTruncWord(text[1:], trunRegList) 
            next_stopPos += 1
            if next_stopPos > 1:
                # print("2 next_stopPos > 1:",text[:next_stopPos].strip())
                return text[:next_stopPos].strip()
            else:
                # print("3 next_stopPos < 1:",text)
                return text
        else:
            # print("4 first_stopPos < subjectLength:",text)
            return text

    def _mergeText(self, subject, textPlain, textHtml, parseFrom = 'textHtml'):
        subject = subject if subject is not None else ''
        textPlain = textPlain if textPlain is not None else ''
        textHtml = textHtml if textHtml is not None else ''
    
        text = '' 
        
        if parseFrom == 'textPlain': ## ---- focus on textPlain (but some ICs will add line breaks in textPlain based on certain width, only used with complete_repley emails from Provetclould )
            if pd.notna(textPlain) and (textPlain != '') and textPlain not in ['Your email client can not display html','']:
                text = textPlain
            elif pd.notna(textHtml) and (textHtml != ''):
                text = self._getTextFromHtml(textHtml)

        else: ## ---- focus on textHtml so no any line breaks, but some Provet Cloud journal email will lose the 'Logo' line, so need to change ehe clinic  PCReg  
            if pd.notna(textHtml) and (textHtml != ''):
                text = self._getTextFromHtml(textHtml)
            elif pd.notna(textPlain) and (textPlain != ''):
                text = textPlain
        
        text = '[SUBJECT]' + subject + '\n' + '[BODY]' + text
        text = self._cleanText(text)
        body = self._truncate(text.strip(), self.stopWords)
            
        return text, body
    
    def _expandMatchingClinic(self, text, keywordList):  
        if not isinstance(text, str):
            return None
    
        for _, row in keywordList.iterrows():
            clinicName = row['clinicName']
            keywords = row['keyword']

            if isinstance(keywords, list):
                if len(keywords) == 1 and keywords[0].lower() in text.lower():
                    return clinicName
                elif len(keywords) > 1 and all(keyword.lower() in text.lower() for keyword in keywords):
                    return clinicName
        return None 

    def _identifyDetailsFromProvetCloud(self, text, errandId):   
        email, sender, recipient = '', 'Provet_Cloud', None
        msg = self._baseMatch(text, self.msgPCReg)

        if msg:
            email = reg.sub(r'\[BODY\].*', f'[BODY]{msg}', text, flags=reg.DOTALL)
        else:
            email = reg.sub(r'\[BODY\].*', f'[BODY]Provet_Cloud blank msg', text, flags=reg.DOTALL)
        
        if len(errandId) == 0:
            clinicName = self._baseMatch(text, self.clinicPCReg)
            if clinicName:
                matchedClinic = self._expandMatchingClinic(clinicName, self.clinicPC)
                if matchedClinic:
                    sender = matchedClinic
            
            if ('If Skadeförsäkring' in text) or (reg.search(r'KD\d*-\d*-?\d*', text)):
                recipient = 'If'
            elif reg.search(r'och FF\d+S', text) or reg.search(r'CV-?\d*-?\d*', text):
                recipient = 'Folksam'
            elif reg.search (r'VOFF-?\w*-?\d*', text):
                recipient = 'Lassie'
            elif reg.search (r'HU\d{2}-', text) or reg.search (r'eller försäkringsnummer \d+', text):
                recipient = 'Svedea'
            else:
                recipient = self._baseMatch(text, self.recipientPCReg)
                if recipient:
                    if ('many' in recipient.lower()) and ('pets' in recipient.lower()):
                        recipient = 'Many Pets' 
                    for keyword, mapped_recipient in self.receiver_mappings.items():
                        if keyword in recipient.lower():
                            recipient = mapped_recipient
                            break
        elif len(errandId) == 1:
            pc = fetchFromDB(self.errandInfoQuery.format(COND=f"er.id = {errandId[0]}"))
            sender = pc['clinicName'].iloc[0]
            recipient = pc['insuranceCompany'].iloc[0] 
         
        return pd.Series({'email': email, 'sender': sender, 'recipient': recipient})
    
    def _findSenderForWisentic(self, text): # Wisentic only be for If, Sveland and Svedea
        sender = 'Wisentic'
        if ('if@djurskador.se' in text) or ('djurskador@if.se' in text) or ('If Skadeförsäkring' in text):
            sender = 'If'
        elif ('kontakta oss på Sveland' in text) or ('@sveland.se' in text) or ('sveland@' in text):
            sender = 'Sveland'
        # elif 'Ingen gällande försäkring finns' in text:
        #     sender = 'Svedea'
            
        return sender

    def _parseEmailAddress(self, emailStrings):
        emailPattern = r'[a-zåöä0-9._%+-]+@[a-zåöä0-9.-]+\.[a-zåöä]{2,}'
        if isinstance(emailStrings, list):
            emailStrings = ','.join([s.strip() for s in emailStrings])  # 对每个元素使用 strip   
        else:
            emailStrings = emailStrings if emailStrings is not None else ""

        emailAdds = reg.findall(emailPattern, emailStrings.lower())
    
        return emailAdds
        
    def _processEmailText(self, df):  
        if df['createdAt'].dtype in ['int', 'int64']: 
            df['createdAt'] = pd.to_datetime(df['createdAt'], unit='ms', utc=True).dt.tz_convert('Europe/Stockholm')
        else:  
            df['createdAt'] = pd.to_datetime(df['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm') 
        df = df.rename(columns={"createdAt":"date"})

        ################## fix 'source' and 'originSender' by 'parsedFrom'
        df = self._fixSource(df)

        # since 'source' confirmed, so can fix 'forwardAddress' and 'capturedIc', but 'Wisentic' should wait for errand has connected
        mask_provetCloud = (df['originSender'] == 'Provet_Cloud') & (df['source'] == 'Clinic')        
        if mask_provetCloud.any():
            df[['origin','email']] = df.apply(lambda row: self._mergeText(row['subject'], row['textPlain'], row['textHtml'], parseFrom='textPlain'), axis=1).apply(pd.Series)
        else:
            df[['origin','email']] = df.apply(lambda row: self._mergeText(row['subject'], row['textPlain'], row['textHtml']), axis=1).apply(pd.Series)

        ################## fix 'sendTo' and 'originRecipient' by 'parsedto'
        df = self._fixSendTo(df)
        
        return df

    def _fixSource(self, df): # source : ['Insurance_Company', 'Clinic', 'DRP', 'Finance', 'Other']
        df['source'], df['originSender'] = 'Other', None
        df['parsedFrom'] = df['from'].apply(self._parseEmailAddress).apply(lambda x: x[0] if x else '') 
        
     # ------------- Source: IC ---------------
        # If 'parsedFrom' contains any IC emails ---->  'originSender' and 'source' are IC
        mask_fromContainsIc = (df['source'] == 'Other') & (df['parsedFrom'].str.lower().str.contains(self.icRefStr, na=False))
        if mask_fromContainsIc.any():
            df.loc[mask_fromContainsIc, 'source'] = 'Insurance_Company'
            df.loc[mask_fromContainsIc & (df['originSender'].isna()), 'originSender'] = df.loc[mask_fromContainsIc & (df['originSender'].isna()),'parsedFrom'].apply(
                lambda x: next((item.capitalize() for item in self.icRefList if isinstance(x, str) and item in x), None))        
        df.loc[(df['source'] == 'Insurance_Company'), 'originSender'] = df.loc[(df['source'] == 'Insurance_Company'), 'originSender'].replace({
            'Trygghansa':'Trygg-Hansa',
            'Mjoback':'Mjöbäcks Pastorat Hästförsäkring',
            'Manypets':'Many Pets',
            'Moderna':'Moderna Försäkringar',
            'Dina':'Dina Försäkringar',
            'Ica':'ICA Försäkring'})
        df.loc[(df['source'] == 'Insurance_Company') & (df['parsedFrom'].isin(['djurskador@djurskador.se', 'wisentic'])), 'originSender'] = 'Wisentic'
        
     # ------------- Source: Clinic ---------------     
        mask_fromClinic = (df['source'] == 'Other')
        if mask_fromClinic.any():
            clinic_data = pd.merge(df.loc[mask_fromClinic], self.clinicList[['clinicName', 'clinicEmail']], left_on='parsedFrom', right_on='clinicEmail', how='left')
            df.loc[mask_fromClinic, 'originSender'] = clinic_data['clinicName']
        
        # Attempt to expand matching clinic if 'originSender' is still missing
        df.loc[df['originSender'].isna(), 'originSender'] = df.loc[df['originSender'].isna()].apply(
            lambda row: self._expandMatchingClinic(row['parsedFrom'], self.clinicKeyWords), axis=1)
        df.loc[df['originSender'].isna() & (df['parsedFrom'].str.lower().str.contains('mailer.provet.email', na=False)), 'originSender'] = 'Provet_Cloud'
        df.loc[df['originSender'].notna() & (df['source'] != 'Insurance_Company'),'source'] = 'Clinic'
        
     # ------------- Source: DRP ---------------
        # If 'parsedFrom' contains any DRP emails ---->  'originSender' and 'source' are DRP
        mask_fromContainsDrp = (df['source'] == 'Other') & (df['parsedFrom'].str.lower().str.contains(self.drpStr, na=False))
        if mask_fromContainsDrp.any():
            df.loc[mask_fromContainsDrp, 'source'] = 'DRP'
            df.loc[mask_fromContainsDrp & (df['originSender'].isna()), 'originSender'] = 'DRP'
        
     # ------------- Source: Finance ---------------
        # If 'parsedFrom' contains any Finance emails ---->  'originSender' and 'source' are Finance
        mask_finance = (df['source'] == 'Other') & (df['parsedFrom'].str.lower().str.contains('fortus|payex', na=False))
        if mask_finance.any():
            df.loc[mask_finance, 'source'] = 'Finance'
        df.loc[df['source']=='Finance', 'originSender'] = df.loc[df['source']=='Finance', 'parsedFrom'].apply(
            lambda x: next((item.capitalize() for item in ['fortus', 'payex'] if isinstance(x, str) and item in x), None))

     # ------------- Source: Other & OriginSender: Postmark ---------------
        # If 'parsedFrom' contains any Postmark emails ---->  'originSender' is Postmark and 'source' are 'Other
        mask_postmark = (df['source'] == 'Other') & df['originSender'].isna() & (df['parsedFrom'].str.contains('@postmarkapp'))
        if mask_postmark.any():
            df.loc[mask_postmark, 'originSender'] = 'Postmark'
     # ------------- return
        return df
    
    def _fixSendTo(self, df): # sendTo : ['Insurance_Company', 'Clinic', 'DRP', 'Finance', 'Other']
        df['parsedTo'] = df['to'].apply(self._parseEmailAddress)
        df[['clinicCompType','capturedIc']] = df.apply(lambda row: pd.Series(self._checkForwardPart(row['source'],row['origin'])), axis=1)

        exploded = df.explode('parsedTo')
        exploded['sendTo'], exploded['originRecipient'], exploded['reference'], exploded['category'] = 'Other', None, None, None
        exploded['errandId'] = exploded.apply(lambda x: [], axis=1)
        
        # ------------- SendTo: Clinic -------------
        # Extract clinic names from 'parsedTo', maybe up to 90%
        exploded = pd.merge(exploded, self.clinicList[['clinicName','clinicEmail']], left_on='parsedTo', right_on='clinicEmail', how='left')
        exploded.loc[exploded['clinicName'].isna(), 'clinicName'] = exploded.loc[exploded['clinicName'].isna()].apply(
            lambda row: self._expandMatchingClinic(row['parsedTo'], self.clinicKeyWords), axis=1)
        exploded['originRecipient'] = exploded['clinicName']
        exploded = exploded.drop('clinicName', axis=1)
        exploded.loc[(exploded['originRecipient'].notna()) & (exploded['source']!='Clinic'), 'sendTo'] = 'Clinic'
        
        # ------------- SendTo: Insurance_Company -------------
        # special address
        mask_specAdd = ((exploded['sendTo']=='Other') & (exploded['parsedTo'].str.lower().str.contains(r'mail\+\d+@drp\.se', na=False, regex=True)))
        if mask_specAdd.any():
            exploded.loc[mask_specAdd, ['source', 'sendTo', 'category']] = ['Clinic', 'Insurance_Company', 'Complement_DR_Clinic']
            specialAdd = self._specialEmailAdd(exploded[mask_specAdd].reset_index(drop=True))
            exploded.loc[mask_specAdd, ['originSender', 'originRecipient', 'reference']] = specialAdd[['clinicName', 'insuranceCompany', 'reference']].values
            exploded.loc[mask_specAdd, 'errandId'] = specialAdd['errandId'].values

        # normal IC
        mask_sendToIC = (exploded['source']=='Clinic') & (exploded['sendTo']=='Other')
        if mask_sendToIC.any():
            exploded.loc[mask_sendToIC, 'originRecipient'] = exploded.loc[mask_sendToIC, 'capturedIc']
            exploded.loc[(exploded['source']=='Clinic') & (exploded['sendTo']=='Other'), 'sendTo'] = 'Insurance_Company'
        
        mask_clinicToIC = (exploded['source'] == 'Clinic') & (exploded['sendTo']=='Insurance_Company')
        if mask_clinicToIC.any():
            exploded.loc[mask_clinicToIC, 'originRecipient'] = exploded.loc[mask_clinicToIC, 'originRecipient'].replace({
                'Trygghansa':'Trygg-Hansa',
                'Mjoback':'Mjöbäcks Pastorat Hästförsäkring',
                'Manypets':'Many Pets',
                'Moderna':'Moderna Försäkringar',
                'Dina':'Dina Försäkringar',
                'Ica':'ICA Försäkring'})
        
        # ------------- SendTo: Drp (have to after Special Address)-------------
        mask_sendToDrp = (exploded['sendTo']=='Other') & (exploded['parsedTo'].str.lower().str.contains(self.drpStr, na=False)) 
        if mask_sendToDrp.any():
            exploded.loc[mask_sendToDrp, ['originRecipient','sendTo']] = ['DRP', 'DRP']
        
        # ------------- SendTo: Finance -------------
        mask_sendToFinan = (exploded['sendTo']=='Other') & (exploded['parsedTo'].str.lower().str.contains(r'fortus|payex', na=False))
        if mask_sendToFinan.any():
            exploded.loc[mask_sendToFinan, 'originRecipient'] = exploded.loc[mask_sendToFinan, 'parsedTo'].apply(
                lambda x: next((item.capitalize() for item in ['fortus', 'payex'] if isinstance(x, str) and item in x.lower()), None))
            exploded.loc[mask_sendToFinan, 'sendTo'] = 'Finance'
        
        # ------------- Drop empty rows, group, and merge back to the main DataFrame
        subset = []
        for col in ['sendTo','originRecipient','reference', 'errandId', 'category']:
            if col in exploded.columns:
                subset.append(col)
        filtered = exploded.dropna(how='all', subset=subset)
        
        agg_dict = {}
        for col in subset:
            if col == 'errandId':
                agg_dict[col] = lambda x: list(set(item for sublist in x for item in (sublist if isinstance(sublist, list) else [sublist])))
            else:
                agg_dict[col] = lambda x: list(set(x))
        grouped = filtered.groupby('id').agg(agg_dict).reset_index()
        
        df = pd.merge(df, grouped, on='id', how='left')
        for col in ['originRecipient', 'sendTo', 'reference', 'category']:
            df[col] = df[col].apply(lambda x: [item for item in x if item is not None] if isinstance(x, list) else [])
            if col == 'sendTo':
                df[col] = df[col].apply(lambda x: ','.join([item for item in x if item != 'Other']) if isinstance(x, list) and any(item != 'Other' for item in x)
                                        else 'Other' if isinstance(x, list)  # Retain 'Other' if it's the only value or list is empty
                                        else x)  # Default to original value (which might already be 'Other')                      
            else:
                df[col] = df[col].apply(lambda x: ','.join(x) if isinstance(x, list) and x else None)
        
        # ------------- return
        
        return df
    
    def _specialEmailAdd(self, df):
        df['reference'] = df['parsedTo'].str.extract(r'mail\+(\d+)@drp\.se')[0]

        refList = df['reference'].dropna().unique().tolist()
        refList = ",".join(f"'{ref}'" for ref in refList)
        refDB = fetchFromDB(self.errandInfoQuery.format(COND=f"ic.reference IN ({refList})"))
        df = pd.merge(df[['id','reference']], refDB, on='reference', how='left')

        return df[['clinicName','insuranceCompany', 'reference', 'errandId']]
 
    def main(self, df_json):
        df = pd.DataFrame(df_json)
        df = self._processEmailText(df)
        
        # process Provet_Cloud
        df['sender'], df['recipient'] = df['originSender'], df['originRecipient']        
        mask_provetCloud = (df['originSender'] == 'Provet_Cloud')
        if mask_provetCloud.any():
            df.loc[mask_provetCloud, ['email', 'sender', 'recipient']] = df.loc[mask_provetCloud, ['email', 'errandId']
                ].apply(lambda row: self._identifyDetailsFromProvetCloud(row['email'], row['errandId']), axis=1)       
                
        # process Wisentic
        mask_sender_wisentic = (df['originSender'] == 'Wisentic')
        if mask_sender_wisentic.any():
            df.loc[mask_sender_wisentic, 'sender'] = df.loc[mask_sender_wisentic, 'email'].apply(self._findSenderForWisentic)
        
        mask_recipient_wisentic = (df['originRecipient'] == 'Wisentic')
        if mask_recipient_wisentic.any():
            df.loc[mask_recipient_wisentic, 'recipient'] = df.loc[mask_recipient_wisentic, 'email'].apply(self._findSenderForWisentic)
        
        df['insuranceCaseRef'] = df['reference']
        df = df.sort_values("date")
        
        # print("******\n",df[['id','from','originSender','sender','source','to','sendTo','originRecipient','recipient','errandId','reference']].iloc[0])
        return df[['id','date','from','originSender','sender','source','to','originRecipient','recipient','sendTo','clinicCompType','reference','insuranceCaseRef','errandId','category','subject','origin','email','attachments']]  