import json
import base64
import fitz  
import regex as reg 
import pandas as pd
from html import escape
from bs4 import BeautifulSoup
from typing import Dict, Any
from .utils import base_match
from .processor import Processor

class Extractor(Processor):
    def __init__(self):
        super().__init__()
 
    def extract_clinic_by_kws(self, hint): # pass
        """
        Expand matching clinic by keywords.
        """
        hint = hint.lower()
        for _, row in self.clinic_keyword.iterrows():
            kws = row['keyword']
            if (len(kws)==1 and kws[0].lower() in hint) or (len(kws)>1 and all(k.lower() in hint for k in kws)):
                return row['clinicName']
        return None
   

    def extract_and_format_number(self, text: str, regex_df: pd.DataFrame, col_group: str) -> Any:
        regex_list = regex_df['regex'].to_list()
        matched_value = base_match(text, regex_list) 
        amount_cols = ['settlementAmount', 'attach_settlementAmount', 'totalAmount', 'attach_totalAmount', 'folksamOtherAmount']
        
        if not matched_value:
            if col_group in amount_cols:
                return pd.NA
            else:
                return None
            
        matched_value = self.clean_email_text(matched_value) 
        name_cols = ['animalName', 'attach_animalName', 'animalName_Sveland', 'ownerName', 'attach_ownerName']
        
        if col_group in name_cols:
            matched_value = reg.sub(r'\(Hund\)|\(hund\)|\(Katt\)|\(katt\)', '', matched_value)
            matched_value = reg.sub(r'[,._\-()/*\s]+', ' ', matched_value)
            matched_value = reg.sub(r'[^a-zA-ZåäöÅÄÖ\'"´ ]', '', matched_value).strip()
            return matched_value or None
            
        elif col_group in amount_cols:
            cleaned_val = matched_value.replace(',00', '').replace('.', '').replace(',', '').replace(' ', '')
            return pd.to_numeric(cleaned_val, errors='coerce')
            
        else:
            return reg.sub(r'\n', '', matched_value).strip()


    def extract_numbers_from_email(self, df: pd.DataFrame) -> pd.DataFrame:
        number_cols = self.number_reg_list['number'].unique()
        
        for col in number_cols:
            if col == 'reference' and df[col].notna().all():
                continue
            
            mask_sveland = (df['originSender'] == 'Sveland')
            process_mask = (df[col].isna()) & (mask_sveland if col == 'animalName_Sveland' else True)
            
            if not process_mask.any():
                continue

            col_regex_df = self.number_reg_list[self.number_reg_list['number'] == col]
            
            extracted_values = df.loc[process_mask, 'origin'].apply(
                lambda text: self.extract_and_format_number(text, col_regex_df, col)
            )
            
            df.loc[process_mask, col] = extracted_values
            
        df['animalName'] = df['animalName'].fillna(df['animalName_Sveland'])
        animal_name_replacements = {
            "Old Smuggler's Marshmallow Peeps": "Nelson",
            "S'Nattsmygarn By the River": "Pixi",
            "Riding's Otto": "Albert"
        }
        df['animalName'] = df['animalName'].replace(animal_name_replacements)

        return df
   
    
    def extract_numbers_from_attach(self, df: pd.DataFrame) -> pd.DataFrame:
        """extract info from attachments"""
        
        mask = df['sender'].isin(['Sveland', 'Svedea'])
        if mask.any():
            new_data_df = df.loc[mask].apply(self.parse_single_pdf, axis=1)
            if not new_data_df.empty:
                df.update(new_data_df)
                
        return df
   
    
    def parse_single_pdf(self, row: pd.Series) -> pd.Series:
        attachments_json = row['attachments']
        if not isinstance(attachments_json, (str, list)):
            return row

        try:
            atts = json.loads(attachments_json) if isinstance(attachments_json, str) else attachments_json
            atts = [{k.lower(): v for k, v in att.items()} for att in atts]
        except (json.JSONDecodeError, TypeError):
            return row

        for att in atts:
            if att.get('name', '').endswith('.pdf') and att['name'].startswith(('claimpaymenttemplate', 'Skadespecifikation')):
                try:
                    decoded_content = base64.b64decode(att['content'])
                    with fitz.open("pdf", decoded_content) as pdf_doc:
                        all_text = "".join(page.get_text() for page in pdf_doc)
                    attach_data = self.get_row_attach_data(all_text, self.attach_reg_list, 'number')
                    for col, value in attach_data.items():
                        if col in row.index:
                            row[col] = value                    
                    settlement_amount = attach_data.get('settlementAmount')
                    if pd.notna(settlement_amount) and settlement_amount >= 0:
                        row['category'] = 'Settlement_Approved'                       
                except Exception: 
                    continue
        return row
   
    
    def get_row_attach_data(self, text: str, regex_df: pd.DataFrame, group_col: str) -> Dict[str, Any]:
        extracted_data = {}
        for col, group in regex_df.groupby(group_col):
            col_name = col.split('_')[-1]
            matched_value = self.extract_and_format_number(text, group, col)
            if matched_value is not None:
                extracted_data[col_name] = matched_value
        return extracted_data
    
    
    def extract_forward_attachments(self, html: str, text: str) -> str:
        """Extract attachment links from HTML"""
        if not html or not html.strip():
            return text
        
        attachment_list = []
        soup = BeautifulSoup(html, 'html.parser')
        pattern = r'(?:^|\s)(attachments|intercom-attachments)(?=\s|$)'
        attachments_table = soup.find('table', class_=reg.compile(pattern))
        
        if attachments_table:
            for a_tag in attachments_table.find_all('a', class_=reg.compile(r'(?:^|\s)intercom-attachment(?:\s|$)')):
                href = a_tag.get('href', '')
                filename = escape(a_tag.get_text(strip=True))
                if href.startswith(('http://', 'https://')) and filename:
                    attachment_list.append(f'<a href="{href}" target="_blank" rel="noopener">{filename}</a>')
        
        if attachment_list:
            return text + "\n[Attachment]: " + "\n".join(attachment_list)
        
        return text
    
    
    