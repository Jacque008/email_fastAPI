import os
import math
import base64
import gspread
import pandas as pd
from typing import Optional, List, TypeVar, Type, Mapping, Any, Union
import regex as reg
from functools import lru_cache
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from groq import Groq
from google.cloud.sql.connector import Connector, IPTypes
from sqlalchemy.sql import text as sqlalchemy_text

@lru_cache(maxsize=3)
def load_sheet_data(url, worksheet, useCols=None):
    try:
        if os.getenv('ENV_MODE') == 'local':
            service_account_file = "data/other/drp-system-73cd3f0ca038.json"
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
    except Exception as e:
        print(f"❌ Error in load_sheet_data: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    return df

# def get_clinic():
#     url = "https://docs.google.com/spreadsheets/d/15TqXNr9UHx4BM8Ae9DbWiFmb_kERbBO1n8u_Oph7LHg/edit?gid=330037605#gid=330037605"
#     worksheet = "clinic"
#     return load_sheet_data(url, worksheet)
 
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

def get_data_from_cloud_engine(db_user, db_password, db_name, query):
    INSTANCE_CONNECTION_NAME = os.getenv('INSTANCE_CONNECTION_NAME', 'drp-system:europe-west4:drp')

    connector = Connector()

    def getconn():
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,  # '/cloudsql/<project_id>:<region>:<instance_name>'
            "pg8000",
            user=db_user,
            password=db_password,
            db=db_name,
            ip_type=IPTypes.PUBLIC  # IPTypes.PRIVATE for private IP
        )
        return conn

    pool = create_engine("postgresql+pg8000://",creator=getconn)

    with pool.connect() as db_conn:
        result = db_conn.execute(sqlalchemy_text(query))
        data = result.fetchall()
        data = pd.DataFrame(data, columns=result.keys()) # type: ignore

    connector.close()

    return data
         
def fetchFromDB(query):
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    
    if os.getenv("ENV_MODE") == "local":
        db_password_encoded = os.getenv('DB_PASSWORD')
        if db_password_encoded:
            db_password = base64.b64decode(db_password_encoded).decode('utf-8')
        else:
            raise ValueError("DB_PASSWORD environment variable is not set")
        data = get_data_from_local_engine(db_user, db_password, db_host, db_port, db_name, query)
    elif os.getenv("ENV_MODE") in ['test','production']:
        db_password = os.getenv('DB_PASSWORD')
        if db_password:
            pass
        else:
            raise ValueError("DB_PASSWORD environment variable is not set")
        data = get_data_from_cloud_engine(db_user, db_password, db_name, query)

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
            return matched.group(1)
    return None   

T = TypeVar("T", bound=BaseModel)
def model_to_dataframe(input: Union[T, List[T]]) -> pd.DataFrame:
    """Convert Pydantic model object(s) to pandas DataFrame"""
    if isinstance(input, list):
        # Handle list of models (original behavior)
        if not input:
            return pd.DataFrame()
        dicts = [model.model_dump() for model in input]
        return pd.DataFrame(dicts)
    else:
        # Handle single model
        dict = input.model_dump()
        return pd.DataFrame([dict])
    
def dataframe_to_model(df: pd.DataFrame, model_cls: Type[T], rename: Optional[Mapping[str, str]] = None,       
    defaults: Optional[Mapping[str, Any]] = None, drop_unknown: bool = True, nan_as_none: bool = True,                         
) -> List[T]:
    fields = (
        set(getattr(model_cls, "model_fields").keys())
        if hasattr(model_cls, "model_fields")
        else set(getattr(model_cls, "__fields__").keys())
    )

    if rename:
        df = df.rename(columns=rename)

    records = df.to_dict(orient="records")
    out: List[T] = []

    for i, rec in enumerate(records):
        if nan_as_none:
            rec = {k: (None if (isinstance(v, float) and math.isnan(v)) else v) for k, v in rec.items()}

        if drop_unknown:
            rec = {k: v for k, v in rec.items() if k in fields}

        if defaults:
            for k, v in defaults.items():
                if rec.get(k) is None:
                    rec[k] = v

        try:
            obj = model_cls(**rec) # type: ignore[arg-type]
        except Exception as e:
            raise ValueError(f"Row {i} cannot be parsed into {model_cls.__name__}: {e}\nData: {rec}") from e

        out.append(obj)

    return out

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
    
def tz_convert(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    if not df.empty:
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce', utc=True).dt.tz_convert('Europe/Stockholm')
        return df
    return pd.DataFrame() 

# def upload_to_gcs(local_path: str, bucket_name: str, dest_path: str):
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(dest_path)
#     blob.upload_from_filename(local_path)
 
def skip_thinking_part(model: str, content: str) -> str:
    """
    Format AI response content based on model type.
    
    Args:
        model: The name of the AI model
        content: The raw AI response content
        
    Returns:
        str: Formatted content with reasoning removed for DeepSeek models
    """
    if model.lower().startswith('deepseek'):
        reasoning_patterns = [
            r'<think>.*?</think>\s*',
            r'<thinking>.*?</thinking>\s*',
            r'<reasoning>.*?</reasoning>\s*',
            r'<analysis>.*?</analysis>\s*'
        ]
        
        result = content
        for pattern in reasoning_patterns:
            result = reg.sub(pattern, '', result, flags=reg.DOTALL | reg.IGNORECASE)

        result = reg.sub(r'^.*?(?:thinking|reasoning|analysis).*?\n\n', '', result, flags=reg.DOTALL | reg.IGNORECASE | reg.MULTILINE)
        result = result.strip()
    else:
        # Only remove common English AI prefixes, not Swedish introductory text
        english_prefixes = [
            r'^Summary:\s*',
            r'^Here is a summary:\s*',
            r'^Here\'s a summary:\s*',
            r'^The summary is:\s*'
        ]

        result = content
        for prefix in english_prefixes:
            result = reg.sub(prefix, '', result, flags=reg.IGNORECASE | reg.MULTILINE)

        result = result.strip()

    return result

def get_groq_client() -> Groq:
    """Get cached Groq client or create new one"""
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        raise ValueError("GROQ_API_KEY environment variable is not set")
    groq_client = Groq(api_key=api_key)
    return groq_client

    
def groq_chat_with_fallback(groq_client: Groq, messages: list, current_model: str) -> tuple[str, str]:
    """
    Make Groq API call with automatic model switching on rate limits.
    
    Args:
        groq_client: Groq client instance
        messages: List of message dictionaries for the API call
        current_model: Current model being used
        
    Returns:
        Tuple of (response_content, used_model)
    """
    models = ["deepseek-r1-distill-llama-70b", "llama-3.3-70b-versatile"]
    models_to_try = [current_model] + [m for m in models if m != current_model]
    
    for model_name in models_to_try:
        try:
            chat_completion = groq_client.chat.completions.create(
                messages=messages,
                model=model_name,
            )
            content = chat_completion.choices[0].message.content
            return content or "", model_name
            
        except Exception as e:
            error_msg = str(e).lower()
            if 'rate limit' in error_msg:
                continue
            else:
                raise e
    
    raise Exception("All models hit rate limits")