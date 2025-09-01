import regex as reg
import pandas as pd
from bs4 import BeautifulSoup
from html import escape
from .preprocess import PreProcess
from .utils import fetchFromDB

# Forwarding API
# URL: https://classify-emails-596633500987.europe-west4.run.app/forward_api
# Input Data (Only one at a time):
    # [{
    #     "id": 135591,
    #     "recipient": "",
    #     "row['correctedCategory']":"",
    #     "userId": 1,
    #     }]
# Output Data (will be also one at a time):
    # [{
    #     "id": 135591,
    #     "forwardAddress": "",
    #     "forwardSubject": "",
    #     "forwardText": "",
    #     }]
    
class CreateForwarding(PreProcess):
    def __init__(self):
        super().__init__()
        self.forwardFormat = pd.read_csv(f"{self.folder}/forwardFormat.csv")
        self.trunList = self.forwardSugg[self.forwardSugg['action']=='Trim'].templates.to_list() 
        self.forwSubList = self.forwardSugg[self.forwardSugg['action']=='Forward_Subject'].templates.to_list()
        self.subList = self.forwardSugg[self.forwardSugg['action']=='Subject'].templates.to_list()
        self.icForwAdd = self.ic[:17].set_index('insuranceCompany')['forwardAddress'].to_dict()
        self.clinicForwAdd = self.clinic.loc[self.clinic['role'] == 'main_email', ['clinicName','clinicEmail']].drop_duplicates()
        self.forward_info_query = self.queries['forwardSummaryInfo'].iloc[0] # keep for paymentMatching and app.py 
        
    def _handleColon(self, text):
        keyWords = ['Ägarens namn.','Djurägare:','Ägarens namn:','Djurägarens namn:','Ägare:','Djur:','Djurets namn:',
                    'Referensnummer:','Journalnummer:','Djurförsäkring:','Klinik:','Försäkringsnummer:','Betalningsreferens 1000',
                    'Namn:','Djurslag:','Journalnr/kundnr:','Direktregleringsnr/journalnr:','Skadenummer:','Patientnummer/journalnummer/kundnummer:','Svelands skadenummer:']
        sentences = text.split("\n")
        
        for i in range(len(sentences) - 1):
            current = sentences[i]
            next_sentence = sentences[i + 1]

            if any(k1 in current for k1 in keyWords) and any(k2 in next_sentence for k2 in keyWords) :
                sentences[i] = current + "§"
                
            if ('nedanstående komplettering:' in sentences[i]) or ('behöver vi få veta följande:' in sentences[i]):
                sentences[i] = sentences[i].replace(":",":§")

        text = "\n".join(sentences)
        # print(f"\n\n** _handleColon **:\n{text}\n")
        return text
    
    def _summarizeInfo(self, row, text):
        info = ''
        if '§' not in text:  
            fields = {'Djurförsäkring: ': 'insuranceNumber',
                      'SkadeNummer: ': 'damageNumber',
                      'Referens: ': 'reference',
                      'Fakturanummer: ': 'invoiceReference',
                      'Djurets namn: ': 'animalName',
                      'Ägarens namn: ': 'ownerName'}
            
            if row['source'] == 'Clinic':
                fields['Klinik: '] = 'sender'
                if row['sendTo'] == 'Insurance_Company':
                    fields['Försäkringsbolag: '] = 'recipient'

            elif row['source'] == 'Insurance_Company':
                fields['Försäkringsbolag: '] = 'sender'
                if row['sendTo'] == 'Clinic':
                    fields['Klinik: '] = 'recipient'
            
            order = ['Klinik: ','Försäkringsbolag: ','Djurförsäkring: ','SkadeNummer: ','Referens: ','Fakturanummer: ','Djurets namn: ','Ägarens namn: ']      
            info = '<br><br><b>Ärendesammanfattning:</b><br>'+'\n'.join(f"{name}{row[fields[name]]}§" for name in order if name in fields and pd.notna(row[fields[name]]))

        return info
    
    def _checkAttachment(self, html, text):
        attachment_list = []
        soup = BeautifulSoup(html, 'html.parser')
        pattern = r'(?:^|\s)(attachments|intercom-attachments)(?=\s|$)'
        attachments_table = soup.find('table', class_=reg.compile(pattern))

        if attachments_table:
            for a_tag in attachments_table.find_all('a', class_=reg.compile(r'(?:^|\s)intercom-attachment(?:\s|$)')):
                href = a_tag.get('href', '')
                filename = escape(a_tag.get_text(strip=True))
                if not href.startswith(('http://', 'https://')):
                    continue
                
                if filename:
                    attachment_list.append(f'<a href="{href}" target="_blank" rel="noopener">{filename}</a>')
        
        if attachment_list:
            separator = "\n[Attachment]: "
            return text +  separator + "\n".join(attachment_list) 
        
        return text
    
    def _cleanBeginning(self, text):
        if reg.search(r'(\[SUBJECT\].*?Vårt ärende:\s*[\d ]+)', text, flags=reg.DOTALL | reg.IGNORECASE):
            text = reg.sub(r'(\[SUBJECT\].*?Vårt ärende:\s*[\d ]+)', '', text, flags=reg.DOTALL | reg.IGNORECASE)
        elif reg.search(r'(\[SUBJECT\].*?Hej)', text, flags=reg.DOTALL | reg.IGNORECASE):
            text = reg.sub(r'(\[SUBJECT\].*?Hej)', 'Hej', text, flags=reg.DOTALL | reg.IGNORECASE)
        else:
            text = reg.sub(r'(\[SUBJECT\].*?\[BODY\])', '', text, flags=reg.DOTALL | reg.IGNORECASE)
            
        return text
        
    def _formating(self, forwardText):
        if pd.isna(forwardText):
            return forwardText
        else:
            text = forwardText.replace('<p>','').replace('</p>','').strip()
            text = reg.sub('\n', '<br><br>', text).strip()
            text = reg.sub(r'<br>(?:\- |\* )(.*?)<br>', r'<br>###<li>\1</li>°°°<br>', text).strip()
            text = reg.sub(r'<br>###', '<ul style="margin-left: 2em;">', text, 1)
            text = reg.sub(r'>rb<°°°', '>rb<>lu/<', text[::-1], 1)[::-1]

            for _, row in self.forwardFormat.iterrows():
                old_text = row['oldText'] if pd.notna(row['oldText']) else ''
                new_text = row['newText'] if pd.notna(row['newText']) else ''
                text = reg.sub(old_text, new_text, text)
                # print(f"** '{old_text}' ** '{new_text}' **:", text)
            return text
        
    def _creating(self, row):
        forwardSubject, forwardText, truncatedText = None, None, None
        info, admin, reference, subject = '', '', '', None
        forwardSubject = self._baseMatch(row['email'], self.forwSubList)   
        if pd.isna(forwardSubject):
            subject = self.forwardSugg.loc[(self.forwardSugg['action'].str.startswith(row['correctedCategory'])) & ((self.forwardSugg['action'].str.endswith('_Subject'))),'templates'].values[0]
            
        truncatedText = self._truncate(row['email'], self.trunList) 
        text = self._handleColon(truncatedText)
        text = self._cleanBeginning(text)
        
        if pd.notna(row['textHtml']) and (row['textHtml'] is not None) and (row['textHtml'].strip() != ''):
            text = self._checkAttachment(row['textHtml'], text)
        
        template = self.forwardSugg.loc[(self.forwardSugg['action'].str.startswith(row['correctedCategory'])) & (self.forwardSugg['action'].str.endswith('_Template')),'templates'].values[0]
        if pd.notna(row['userId']) and (row['userId'] is not None) and (row['userId'] != 0):
            adminDF = fetchFromDB(self.adminQuery.format(COND=f"id = {row['userId']}"))
            admin = adminDF['firstName'].values[0] if not adminDF.empty else ''
            
        info = self._summarizeInfo(row, text)

        if row['correctedCategory'] in ('Wisentic_Error', 'Insurance_Validation_Error'):
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject.format(REFERENCE=row['reference'])
            forwardText = template.format(WHO=row['sender'], EMAIL=text, INFO=info, ADMIN=admin)
            # print("\n1 'Wisentic_Error', 'Insurance_Validation_Error' forwardText:\n", forwardText)
        
        elif row['correctedCategory'] == 'Complement_DR_Insurance_Company':
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject.format(REFERENCE=row['reference'])
            reference = f'&lt;mail+{row["reference"]}@drp.se&gt;' if not row.empty else ''
            forwardText = template.format(REFERENCE=reference, WHO=row['sender'], EMAIL=text, INFO=info, ADMIN=admin)
            # print("\n2 'Complement_DR_Insurance_Company' forwardText:\n", forwardText)
                
        elif row['correctedCategory'] == 'Complement_DR_Clinic':
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject.format(REFERENCE=row['reference']) 
            if 'Provet_Cloud blank msg' in text:
                template = self.forwardSugg.loc[self.forwardSugg['action']=='ProvetCloud_Template','templates'].values[0]
                forwardText = template.format(WHO=row['sender'], INFO=info, ADMIN=admin)
            else:    
                forwardText = template.format(WHO=row['sender'], EMAIL=text, INFO=info, ADMIN=admin)
            # print("\n3 'Complement_DR_Clinic' forwardText:\n", forwardText)
            
        elif row['correctedCategory'] == 'Settlement_Approved':
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject
            forwardText = template.format(WHO=row['sender'], EMAIL=text, ADMIN=admin)
            # print("\n4 'Settlement_Approved' forwardText:\n", forwardText)
            
        elif row['correctedCategory'] == 'Question':
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject.format(WHO=row['sender']) 
            if pd.notna(row['reference']):
                reference = f"eller skicka ett mail med kompletteringen till {row['reference']} "
            forwardText = template.format(REFERENCE=reference, WHO=row['sender'], EMAIL=text, INFO=info, ADMIN=admin)
            # print("\n5 'Question' forwardText:\n", forwardText)
        
        elif row['correctedCategory'] == 'Message':
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject.format(WHO=row['sender']) 
            forwardText = template.format(WHO=row['sender'], EMAIL=text, INFO=info, ADMIN=admin)
            # print("\n6 'Message' forwardText:\n", forwardText)
            
        else:   
            if (pd.isna(forwardSubject)) and (pd.notna(subject)) and (subject != ''):
                forwardSubject = subject
            forwardText = template.format(EMAIL=text, INFO=info, ADMIN=admin) 
            # print("\n7 rest-category forwardText:\n", forwardText)    
             
        forwardText = self._formating(forwardText)

        return forwardSubject.strip(), forwardText.strip()
    
    def main(self, df): 
        df['email'] = df['email'].replace('\n\n+', '\n', regex=True)  
        df = df.drop(['errandId','reference','sender'], axis=1,  errors='ignore')
        df['forwardAddress'], df['forwardSubject'], df['forwardText'] = None, None, None   
        summary = fetchFromDB(self.forward_info_query.format(ID=df['id'].iloc[0]))
        df = pd.merge(df, summary, on=['id', 'recipient'], how='left')
        
        df[['forwardSubject', 'forwardText']] = df.apply(lambda row: self._creating(row), axis=1).apply(pd.Series)
        
        mask_toIc = (df['source'] == 'Clinic')
        df.loc[mask_toIc, 'forwardAddress'] = df['recipient'].map(self.icForwAdd)
        
        df = pd.merge(df, self.clinicForwAdd, left_on='recipient', right_on='clinicName', how='left')
        
        mask_toClinic = (df['source'] == 'Insurance_Company')
        df.loc[mask_toClinic, 'forwardAddress'] = df.loc[mask_toClinic, 'clinicEmail']

        return df[['id', 'forwardAddress', 'forwardSubject', 'forwardText']]

        
    

