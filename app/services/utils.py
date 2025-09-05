import os
import base64
import gspread
import pandas as pd
from typing import Optional, List, Any
import regex as reg
from functools import lru_cache
from sqlalchemy import create_engine, text
from pydantic import BaseModel
# from google.cloud.sql.connector import Connector, IPTypes
# from sqlalchemy.sql import text as sqlalchemy_text

@lru_cache(maxsize=3)
def load_sheet_data(url, worksheet, useCols=None):
    if os.getenv('ENV_MODE') == 'local':
        service_account_file = "data/other/drp-system-73cd3f0ca038.json"
        # service_account_file = "other/pw/drp-system-73cd3f0ca038.json"
    elif os.getenv('ENV_MODE') in ['test', 'production']:
        service_account_file = "/SERVICE_ACCOUNT_JIE/SERVICE_ACCOUNT_JIE" 
    gc = gspread.service_account(filename=service_account_file)
    spreadsheet = gc.open_by_url(url)
    worksheet = spreadsheet.worksheet(worksheet)
    raw_data = worksheet.get_all_values()
    df = pd.DataFrame(raw_data[1:], columns=raw_data[0]) # Set the first row as header and remove it from the data
    df = df.replace('', None)
    if useCols:
        df = df[list(useCols)]

    return df

def get_clinic():
    url = "https://docs.google.com/spreadsheets/d/15TqXNr9UHx4BM8Ae9DbWiFmb_kERbBO1n8u_Oph7LHg/edit?gid=330037605#gid=330037605"
    worksheet = "clinic"
    return load_sheet_data(url, worksheet)

def get_payoutEntity():
    url = "https://docs.google.com/spreadsheets/d/15TqXNr9UHx4BM8Ae9DbWiFmb_kERbBO1n8u_Oph7LHg/edit?gid=1488074124#gid=1488074124"
    worksheet = "payoutEntity"
    return load_sheet_data(url, worksheet)

def get_staffAnimal():
    url = "https://docs.google.com/spreadsheets/d/1XNCiHSX0aSsmfEufB3puWHbNsj3paSzdZWi3bKkyZKs/edit?gid=1745017939#gid=1745017939"
    worksheet = "Har personaldjur"
    useCols = ('Klinik', 'Personal', 'Djur')
    return load_sheet_data(url, worksheet, useCols) 
   
def get_data_from_local_engine(db_user, db_password, db_host, db_port, db_name, query):
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    data = pd.read_sql(text(query), engine)
    
    return data

# def get_data_from_cloud_engine(db_user, db_password, db_name, query): 
#     INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME')

#     connector = Connector()  
#     def getconn():
#         conn = connector.connect(
#             INSTANCE_CONNECTION_NAME,  # '/cloudsql/<project_id>:<region>:<instance_name>'
#             "pg8000",
#             user=db_user,
#             password=db_password,
#             db=db_name,
#             ip_type=IPTypes.PUBLIC  # IPTypes.PRIVATE for private IP
#         )
#         return conn

#     pool = create_engine("postgresql+pg8000://",creator=getconn)        
#     with pool.connect() as db_conn:
#         result = db_conn.execute(sqlalchemy_text(query))
#         data = result.fetchall()
#         data = pd.DataFrame(data, columns=result.keys())
                    
#     connector.close()
    
#     return data
         
def fetchFromDB(query):
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    
    if os.getenv("ENV_MODE") == "local":
        db_password = base64.b64decode(os.getenv('DB_PASSWORD')).decode('utf-8')
        data = get_data_from_local_engine(db_user, db_password, db_host, db_port, db_name, query)
    elif os.getenv("ENV_MODE") in ['test','production']:
        db_password = os.getenv('DB_PASSWORD')
        # data = get_data_from_cloud_engine(db_user, db_password, db_name, query)
        
    return data

def readGoogleSheet(url, worksheet, useCols=None):
    if os.getenv('ENV_MODE') == 'local':
        service_account_file = "other/pw/drp-system-73cd3f0ca038.json"
    elif os.getenv('ENV_MODE') in ['test', 'production']:
        service_account_file = "/SERVICE_ACCOUNT_JIE/SERVICE_ACCOUNT_JIE" 
        
    gc = gspread.service_account(filename=service_account_file)
    spreadsheet = gc.open_by_url(url)
    worksheet = spreadsheet.worksheet(worksheet)
    data = worksheet.get_all_values()
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]  # Set the first row as header
    df = df[1:]  # Remove the header row from the data
    df = df.replace('', None)
    if useCols:
        df = df[useCols]
    
    return df
    
def parse_email_address(emails_adds: str)-> List[str]:
    """
    Parse email addresses from a string or a list of strings.
    """
    if not isinstance(emails_adds, str):
        return []
    
    email_pattern = r'[a-zåöä0-9._%+-]+@[a-zåöä0-9.-]+\.[a-zåöä]{2,}'
    add_str = ','.join(emails_adds) if isinstance(emails_adds, list) else (emails_adds or "")
    return reg.findall(email_pattern, add_str.lower())

def extract_first_address(emails_adds):
    """
    Return the first email address found.
    """
    addresses = parse_email_address(emails_adds)
    return addresses[0] if addresses else ''

def expand_matching_clinic(parsed_from: str, keyword_df: pd.DataFrame) -> Optional[str]:
        if not isinstance(parsed_from, str) or keyword_df is None or keyword_df.empty:
            return None

        hay = parsed_from.lower()
        for _, row in keyword_df[['clinicName', 'keyword']].iterrows():
            clinic = row['clinicName']
            kws = row['keyword']
            if isinstance(kws, list) and kws:  
                if all(isinstance(k, str) and k.lower() in hay for k in kws):
                    return clinic
        return None

def parse_from_column(df, col='from', new_col='parsedFrom'):
    """
    Apply email parsing to a DataFrame column.
    """
    df[new_col] = df[col].apply(extract_first_address)
    return df

def parse_to_column(df, col='to', new_col='parsedTo'):
    """
    Parse multiple addresses from a 'to' column and return exploded DataFrame.
    """
    df[new_col] = df[col].apply(parse_email_address)
    exploded = df.explode(new_col)
    return exploded
 
def base_match(text: str, patterns: List[str]) -> Optional[str]:
    for p in map(lambda r: reg.compile(r, reg.DOTALL|reg.MULTILINE), patterns):
        matched = p.search(text)
        if matched: 
            # print(p)
            # print(matched)
            # print(matched.group(1))
            return matched.group(1)
    return None   

def model_to_dataframe(inputs: List[BaseModel]) -> pd.DataFrame:
    """Convert list of PaymentIn objects to pandas DataFrame"""
    dicts = []
    for input in inputs:
        dict = input.model_dump()
        dicts.append(dict)
    
    return pd.DataFrame(dicts)
    
def check_eq(a, b):
    grp = {'Trygg-Hansa', 'Moderna Försäkringar'}
    if pd.isna(a) or pd.isna(b):
        return False
    return (a in grp and b in grp) or (a == b)
        
def pick_first(df: pd.DataFrame) -> Optional[pd.Series]:
    return None if df.empty else df.iloc[0]

def lower_and_split(s: str) -> set:
    return {w for w in str(s).lower().split() if w}

def part_name_mask(query: str, series: pd.Series) -> pd.Series:
    q = lower_and_split(query)
    if not q:
        return pd.Series(False, index=series.index)
    return series.fillna('').astype(str).str.lower().apply(
        lambda txt: q.issubset(lower_and_split(txt))
    )

def check_full_parts_match(df: pd.DataFrame, col: str, email_name: Optional[str]) -> tuple[list, list]:
    if df is None or df.empty or email_name is None:
        return [], []
    col_lower = df[col].str.lower()
    full_mask = (col_lower == str(email_name).lower())
    full_ids = df.loc[full_mask, 'errandId'].tolist()
    part_mask = ~full_mask & part_name_mask(email_name, col_lower)
    part_ids = df.loc[part_mask, 'errandId'].tolist()
    return full_ids, part_ids

def list_deduplicate(lst: list) -> list:
    return list(dict.fromkeys(lst))

def as_id_list(v):
    if isinstance(v, list):
        return v
    if isinstance(v, tuple):
        return list(v)
    import pandas as pd
    if pd.isna(v):
        return []
    try:
        return [int(v)]
    except Exception:
        return [v]
    
def find_trunc_pos(text, trunc_reg_list):
    stop = len(text)
    for patt in map(lambda r: reg.compile(r, reg.DOTALL | reg.MULTILINE), trunc_reg_list):
        matched = reg.search(patt, text)
        if matched and 0 <= matched.start() < stop:
            stop = matched.start()
    return stop
     
def truncate_text(text, trunc_reg_list):
    subject = text.split('[BODY]', 1)[0]
    subject_len = len(subject + '[BODY]')
    pos1 = find_trunc_pos(text, trunc_reg_list)
    if pos1 > subject_len:
        return text[:pos1].strip()
    elif pos1 == subject_len:
        pos2 = find_trunc_pos(text[1:], trunc_reg_list) + 1
        return text[:pos2].strip() if pos2 > 1 else text
    else:
        return text
    
# def upload_to_gcs(local_path: str, bucket_name: str, dest_path: str):
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(dest_path)
#     blob.upload_from_filename(local_path)
#     print(f"Uploaded to gs://{bucket_name}/{dest_path}")