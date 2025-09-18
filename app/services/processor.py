import html
import regex as reg
import pandas as pd
from inscriptis import get_text
from .base_service import BaseService
from .utils import tz_convert, truncate_text
from pandas.api.types import is_integer_dtype


class Processor(BaseService):
    def adjust_time_format(self, df: pd.DataFrame) -> pd.DataFrame:
        if is_integer_dtype(df['createdAt']):
            df['date'] = pd.to_datetime(df['createdAt'], unit='ms', utc=True).dt.tz_convert('Europe/Stockholm')
        else:
            df['date'] = pd.to_datetime(df['createdAt'], errors='coerce', utc=True).dt.tz_convert('Europe/Stockholm')
        df = df.drop(columns=['createdAt'])

        return df

    
    def clean_email_text(self, text):
        text = html.unescape(text or "")
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
            r'\n ': '\n'
        }
        
        for code, char in special_chars.items():
            text = reg.sub(code, char, text, flags=reg.MULTILINE)

        text = reg.sub(r'\n^[>]+ ?', '\n', text, flags=reg.MULTILINE)
        text = reg.sub(r'_([^_]+)_', r'\1', text)
        text = reg.sub(r'[ \t]+', ' ', text)
        text = reg.sub(r'\n\n+', '\n\n', text)

        return text.strip()

        
    def merge_html_text(self, subject, text_plain, text_html, parse_from='textHtml'):
        subject = subject or ''
        text_plain = text_plain or ''
        text_html = text_html or ''
        
        if parse_from == 'textPlain':
            if text_plain and text_plain not in ['Your email client can not display html', '']:
                body = text_plain
            else:
                body = get_text(text_html)
        else:
            if text_html:
                body = get_text(text_html)
            else:
                body = text_plain

        full_text = self.clean_email_text(f"[SUBJECT]{subject}\n[BODY]{body}")
        body = truncate_text(full_text, self.forward_words)
        body = truncate_text(body, self.stop_words)
        
        return full_text or "", body or ""
    
        
    def generate_email_content(self, df: pd.DataFrame) -> pd.DataFrame:
        required_cols = ['subject', 'textPlain', 'textHtml', 'originSender', 'source']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns for generate_email_content: {missing_cols}")
        
        mask_provetCloud = (df['originSender'] == 'Provet_Cloud') & (df['source'] == 'Clinic')        
        if mask_provetCloud.any():
            try:
                df[['origin','email']] = df.apply(lambda row: self.merge_html_text(row['subject'], row['textPlain'], row['textHtml'], parse_from='textPlain'), axis=1).apply(pd.Series)
            except Exception as e:
                raise
        else:
            try:
                df[['origin','email']] = df.apply(lambda row: self.merge_html_text(row['subject'], row['textPlain'], row['textHtml']), axis=1).apply(pd.Series)
            except Exception as e:
                raise
                 
        return df 
    
    
    
