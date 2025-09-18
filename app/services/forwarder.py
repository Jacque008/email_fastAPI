"""
Forwarder Service - Handle content generation for email forwarding
Extracted from workflow/create_forwarding.py
"""
import regex as reg
import pandas as pd
from typing import Dict
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from html import escape
from typing import List
from .base_service import BaseService
from .utils import truncate_text, base_match

@dataclass
class Forwarder(BaseService):
    """Content generator service for email forwarding"""
    request_fw_sub: List[str] = field(default_factory=list)

    def __post_init__(self):
        super().__init__()  
        self._setup_generation_configs()
    
    def _setup_generation_configs(self):
        try:
            fw_cates = self.forward_suggestion[self.forward_suggestion['action'].str.endswith('_Template')].action.to_list()
            self.fw_cates = [item.replace('_Template', '') for item in fw_cates]
            self.trun_list = self.forward_suggestion[self.forward_suggestion['action']=='Trim'].templates.to_list()
            self.sub_list = self.forward_suggestion[self.forward_suggestion['action']=='Subject'].templates.to_list()
            self.forward_format = pd.read_csv(f"{self.folder}/forwardFormat.csv")
            self.request_fw_sub = self.forward_suggestion[self.forward_suggestion['action']=='Forward_Subject'].templates.to_list()
        except Exception as e:
            self.forward_format = pd.DataFrame()
            

    def generate_forwarding_subject(self, email: str, category: str, **kwargs) -> str:
        """Generate forward subject based on email content and category"""
        try:
            forward_subject = base_match(email, self.request_fw_sub)

            if pd.isna(forward_subject):
                subject_template = self._get_category_subject_template(category)
                if subject_template:
                    return self._format_subject_template(subject_template, category, **kwargs)
            
            return forward_subject.strip() if forward_subject else ""
            
        except Exception as e:
            return ""
    
    def generate_email_content(self, row_data: Dict, admin_name: str = '') -> str:
        """Generate email content based on template and data"""
        try:
            category = row_data.get('correctedCategory', '')
            template = self._get_forwarding_content_template(category)
            email_content = row_data.get('email', '')
            processed_text = self._process_email_text(email_content, row_data.get('textHtml', ''))
            info = self._generate_summary_info(row_data, processed_text)
            forward_text = self._generate_category_specific_content(
                template, processed_text, info, admin_name, category, row_data
            )

            return self._format_forward_text(forward_text)
            
        except Exception as e:
            return ""
     
    def _get_category_subject_template(self, category: str) -> str:
        try:
            templates = self.forward_suggestion.loc[
                (self.forward_suggestion['action'].str.startswith(category)) & 
                (self.forward_suggestion['action'].str.endswith('_Subject')), 'templates'
            ]
            return templates.values[0] if not templates.empty else ""
        except:
            return ""
    
    def _format_subject_template(self, template: str, category: str, **kwargs) -> str:
        """Format subject template with provided parameters"""
        try:
            if category in ('Wisentic_Error', 'Insurance_Validation_Error', 
                           'Complement_DR_Insurance_Company', 'Complement_DR_Clinic'):
                return template.format(REFERENCE=kwargs.get('reference', ''))
            elif category in ('Question', 'Message'):
                return template.format(WHO=kwargs.get('sender', ''))
            else:
                return template
        except:
            return template
    
    def _get_forwarding_content_template(self, category: str) -> str:
        """Get forwarding template for specific category"""
        try:
            templates = self.forward_suggestion.loc[
                (self.forward_suggestion['action'].str.startswith(category)) & 
                (self.forward_suggestion['action'].str.endswith('_Template')), 'templates'
            ]
            return templates.values[0] if not templates.empty else "{EMAIL}"
        except:
            return "{EMAIL}"
    
    def _process_email_text(self, email: str, html_content: str = '') -> str:
        """Process and clean email text content"""
        try:
            text = truncate_text(email, self.trun_list)
            text = self._handel_colon(text)
            text = self._clean_beginning(text)
            if html_content and html_content.strip():
                text = self._check_attachment(html_content, text)
            
            return text
            
        except Exception as e:
            return email

    def _handel_colon(self, text: str) -> str:
        """Handle colon formatting"""
        key_words = [
            'Ägarens namn.', 'Djurägare:', 'Ägarens namn:', 'Djurägarens namn:', 
            'Ägare:', 'Djur:', 'Djurets namn:', 'Referensnummer:', 'Journalnummer:', 
            'Djurförsäkring:', 'Klinik:', 'Försäkringsnummer:', 'Betalningsreferens 1000',
            'Namn:', 'Djurslag:', 'Journalnr/kundnr:', 'Direktregleringsnr/journalnr:', 
            'Skadenummer:', 'Patientnummer/journalnummer/kundnummer:', 'Svelands skadenummer:'
        ]
        
        sentences = text.split("\n")
        
        for i in range(len(sentences) - 1):
            current = sentences[i]
            next_sentence = sentences[i + 1]
            
            if (any(k1 in current for k1 in key_words) and 
                any(k2 in next_sentence for k2 in key_words)):
                sentences[i] = current + "§"
            
            if ('nedanstående komplettering:' in sentences[i] or 
                'behöver vi få veta följande:' in sentences[i]):
                sentences[i] = sentences[i].replace(":", ":§")
        
        return "\n".join(sentences)

    def _clean_beginning(self, text: str) -> str:
        """Clean email beginning"""
        if reg.search(r'(\[SUBJECT\].*?Vårt ärende:\s*[\d ]+)', text, flags=reg.DOTALL | reg.IGNORECASE):
            text = reg.sub(r'(\[SUBJECT\].*?Vårt ärende:\s*[\d ]+)', '', text, flags=reg.DOTALL | reg.IGNORECASE)
        elif reg.search(r'(\[SUBJECT\].*?Hej)', text, flags=reg.DOTALL | reg.IGNORECASE):
            text = reg.sub(r'(\[SUBJECT\].*?Hej)', 'Hej', text, flags=reg.DOTALL | reg.IGNORECASE)
        else:
            text = reg.sub(r'(\[SUBJECT\].*?\[BODY\])', '', text, flags=reg.DOTALL | reg.IGNORECASE)
        
        return text

    def _check_attachment(self, html: str, text: str) -> str:
        """Check and process attachments from HTML content"""
        attachment_list = []
        
        try:
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
                return text + separator + "\n".join(attachment_list)
        except Exception as e:
            pass

        return text
    
    def _generate_summary_info(self, row_data: Dict, text: str) -> str:
        """Generate summary information"""
        info = ''
        if '§' not in text:
            fields = {
                'Djurförsäkring: ': 'insuranceNumber',
                'SkadeNummer: ': 'damageNumber',
                'Referens: ': 'reference',
                'Fakturanummer: ': 'invoiceReference',
                'Djurets namn: ': 'animalName',
                'Ägarens namn: ': 'ownerName'
            }
            
            source = row_data.get('source', '')
            send_to = row_data.get('sendTo', '')
            
            if source == 'Clinic':
                fields['Klinik: '] = 'sender'
                if send_to == 'Insurance_Company':
                    fields['Försäkringsbolag: '] = 'receiver'
            elif source == 'Insurance_Company':
                fields['Försäkringsbolag: '] = 'sender'
                if send_to == 'Clinic':
                    fields['Klinik: '] = 'receiver'
            
            order = ['Klinik: ','Försäkringsbolag: ','Djurförsäkring: ','SkadeNummer: ',
                    'Referens: ','Fakturanummer: ','Djurets namn: ','Ägarens namn: ']
            
            info = '<br><br><b>Ärendesammanfattning:</b><br>' + '\n'.join(
                f"{name}{row_data.get(fields[name], '')}§" 
                for name in order 
                if name in fields and pd.notna(row_data.get(fields[name]))
            )

        return info
    
    def _generate_category_specific_content(self, template: str, text: str, info: str, 
                                          admin: str, category: str, row_data: Dict) -> str:
        """Generate content based on specific category"""
        sender = row_data.get('sender', '') or row_data.get('from', '') or 'Okänd avsändare'
        ref = row_data.get('reference', '') or ''
        
        # Ensure we have fallback values for template formatting
        sender = sender if sender else 'Okänd avsändare'
        admin = admin if admin else 'Admin'
        text = text if text else 'Inget innehåll tillgängligt'
        info = info if info else 'Ingen ytterligare information'
        
        try:
            if category in ('Wisentic_Error', 'Insurance_Validation_Error'):
                return template.format(WHO=sender, EMAIL=text, INFO=info, ADMIN=admin)
            
            elif category == 'Complement_DR_Insurance_Company':
                reference = f'&lt;mail+{ref}@drp.se&gt;' if ref else ''
                return template.format(REFERENCE=reference, WHO=sender, EMAIL=text, INFO=info, ADMIN=admin)
                    
            elif category == 'Complement_DR_Clinic':
                if 'Provet_Cloud blank msg' in text:
                    provet_template = self._get_provet_cloud_template()
                    return provet_template.format(WHO=sender, INFO=info, ADMIN=admin)
                else:    
                    return template.format(WHO=sender, EMAIL=text, INFO=info, ADMIN=admin)
                
            elif category == 'Settlement_Approved':
                return template.format(WHO=sender, EMAIL=text, ADMIN=admin)
                
            elif category == 'Question':
                reference = f"eller skicka ett mail med kompletteringen till {ref} " if ref else ""
                return template.format(REFERENCE=reference, WHO=sender, EMAIL=text, INFO=info, ADMIN=admin)
            
            elif category == 'Message':
                return template.format(WHO=sender, EMAIL=text, INFO=info, ADMIN=admin)
                
            else:   
                return template.format(EMAIL=text, INFO=info, ADMIN=admin)
            
        except Exception as e:
            # Fallback template with all required parameters
            try:
                return template.format(WHO=sender, EMAIL=text, INFO=info, ADMIN=admin, REFERENCE=ref)
            except Exception as fallback_error:
                return f"Hej {sender},\n\n{text}\n\n{info}\n\nMed vänlig hälsning,\n{admin}"
    
    def _get_provet_cloud_template(self) -> str:
        """Get Provet Cloud template"""
        try:
            templates = self.forward_suggestion.loc[self.forward_suggestion['action']=='ProvetCloud_Template', 'templates']
            return templates.values[0] if not templates.empty else "{WHO} {INFO} {ADMIN}"
        except:
            return "{WHO} {INFO} {ADMIN}"
    
    def _format_forward_text(self, forward_text: str) -> str:
        """Format forward text with HTML and styling"""
        if pd.isna(forward_text):
            return forward_text
        
        try:
            text = forward_text.replace('<p>', '').replace('</p>', '').strip()
            text = reg.sub('\n', '<br><br>', text).strip()
            text = reg.sub(r'<br>(?:\- |\* )(.*?)<br>', r'<br>###<li>\1</li>°°°<br>', text).strip()
            text = reg.sub(r'<br>###', '<ul style="margin-left: 2em;">', text, 1)
            text = reg.sub(r'>rb<°°°', '>rb<>lu/<', text[::-1], 1)[::-1]

            # Apply format rules from CSV
            for _, row in self.forward_format.iterrows():
                old_text = row['oldText'] if pd.notna(row['oldText']) else ''
                new_text = row['newText'] if pd.notna(row['newText']) else ''
                if old_text:
                    text = reg.sub(old_text, new_text, text)
            
            return text
        except Exception as e:
            return forward_text or ""
    
