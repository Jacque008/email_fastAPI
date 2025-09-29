import os
import regex as reg
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

class BaseService:
    """
    Base service class for all business services.
    Provides common functionality like data loading and environment configuration.
    """
    _data_cache = {}
    _cache_initialized = False
    _cache_date = None  

    def __init__(self):
        env = os.getenv('ENV_MODE', 'local')  
        self.folder = {
            'local': "data/para_tables",
            'test': "gs://ml_email_test/fastapi",
            'production': "gs://ml_email_category/fastapi"
        }.get(env, "data/para_tables")
        current_date = datetime.now().date()
        cache_expired = BaseService._cache_date != current_date

        if not BaseService._cache_initialized or cache_expired:
            self._load_tables()
            BaseService._cache_initialized = True
            BaseService._cache_date = current_date
        else:
            self._load_from_cache()

    def _load_tables(self):
        """Load all required data tables and configuration files"""

        # Insurance company data
        self.fb = pd.read_csv(f"{self.folder}/fb.csv")
        self.fb_ref_list = self.fb[:-5].insuranceCompanyReference.tolist()
        self.fb_ref_str = '|'.join([reg.escape(fb) for fb in self.fb_ref_list])
        self.drp_str = '|'.join([reg.escape(fb) for fb in self.fb[-5:].insuranceCompanyReference.tolist()])

        # Clinic data
        self.clinic = pd.read_csv(f"{self.folder}/clinic.csv")
        self.clinic_list = self.clinic[['clinicId','clinicName','clinicEmail']].drop_duplicates(subset=['clinicEmail'], keep='last')
        self.clinic_keyword = self.clinic[self.clinic['keyword'].notna()][['clinicName','keyword']].drop_duplicates()
        self.clinic_keyword['keyword'] = self.clinic_keyword['keyword'].apply(lambda x: x.split(',') if isinstance(x, str) else [])

        clinic_comp_type_df = pd.read_csv(f"{self.folder}/clinicCompType.csv")
        self.clinic_complete_type = clinic_comp_type_df.complement.tolist()

        # Text processing data
        stop_words_df = pd.read_csv(f"{self.folder}/stopWords.csv")
        self.stop_words = stop_words_df.stopWords.tolist()
        forward_words_df = pd.read_csv(f"{self.folder}/forwardWords.csv")
        self.forward_words = forward_words_df.forwardWords.tolist()
        self.forward_suggestion = pd.read_csv(f"{self.folder}/forwardSuggestion.csv")
        
        # Provet Cloud specific data
        self.clinic_provetcloud = self.clinic[self.clinic['provetCloud'].notna()][['clinicName','provetCloud']].drop_duplicates()
        self.clinic_provetcloud['keyword'] = self.clinic_provetcloud['provetCloud'].apply(lambda x: x.split(',') if isinstance(x, str) else [])
        self.msg_provetcloud_reg = self.forward_suggestion[self.forward_suggestion['action']=='ProvetCloud_Msg'].templates.to_list()
        self.clinic_provetcloud_reg = self.forward_suggestion[self.forward_suggestion['action']=='ProvetCloud_Clinic'].templates.to_list()
        self.receiver_provetcloud_reg = self.forward_suggestion[self.forward_suggestion['action']=='ProvetCloud_Recipient'].templates.to_list()
        
        # Insurance company mappings
        self.receiver_mappings = {
            'sveland': 'Sveland',
            'agria': 'Agria',
            'dina': 'Dina Försäkringar',
            'trygg': 'Trygg-Hansa',
            'moderna': 'Moderna Försäkringar',
            'ica': 'ICA Försäkring',
            'hedvig': 'Hedvig',
            'dunstan': 'Dunstan',
            'petson': 'Petson'
        }

        # Regex patterns for data extraction
        self.number_reg_list = pd.read_csv(f"{self.folder}/numberReg.csv")
        self.attach_reg_list = pd.read_csv(f"{self.folder}/attachReg.csv")

        # Database queries
        self.queries = pd.read_csv(f"{self.folder}/queries.csv")
        self.email_spec_query = self.queries['emailSpec'].iloc[0]
        self.admin_query = self.queries['admin'].iloc[0]
        self.update_clinic_email_query = self.queries['updateClinicEmail'].iloc[0]
        self.errand_info_query = self.queries['errandInfo'].iloc[0]
        self.model_df = pd.read_csv(f"{self.folder}/model.csv")

        # Category classification data
        self.category_reg_list = pd.read_csv(f"{self.folder}/categoryReg.csv")

        # Forward format data
        self.forward_format = pd.read_csv(f"{self.folder}/forwardFormat.csv")

        # Payment service data
        self.info_reg = pd.read_csv(f"{self.folder}/infoReg.csv")
        self.bank_map = pd.read_csv(f"{self.folder}/bankMap.csv")

        # Cache all loaded data
        BaseService._data_cache = {
            'folder': self.folder,
            'fb': self.fb,
            'fb_ref_list': self.fb_ref_list,
            'fb_ref_str': self.fb_ref_str,
            'drp_str': self.drp_str,
            'clinic': self.clinic,
            'clinic_list': self.clinic_list,
            'clinic_keyword': self.clinic_keyword,
            'clinic_complete_type': self.clinic_complete_type,
            'stop_words': self.stop_words,
            'forward_words': self.forward_words,
            'forward_suggestion': self.forward_suggestion,
            'clinic_provetcloud': self.clinic_provetcloud,
            'msg_provetcloud_reg': self.msg_provetcloud_reg,
            'clinic_provetcloud_reg': self.clinic_provetcloud_reg,
            'receiver_provetcloud_reg': self.receiver_provetcloud_reg,
            'receiver_mappings': self.receiver_mappings,
            'number_reg_list': self.number_reg_list,
            'attach_reg_list': self.attach_reg_list,
            'queries': self.queries,
            'email_spec_query': self.email_spec_query,
            'admin_query': self.admin_query,
            'update_clinic_email_query': self.update_clinic_email_query,
            'errand_info_query': self.errand_info_query,
            'model_df': self.model_df,
            'category_reg_list': self.category_reg_list,
            'forward_format': self.forward_format,
            'info_reg': self.info_reg,
            'bank_map': self.bank_map
        }

    def _load_from_cache(self):
        """Load data from cache instead of reading CSV files"""
        cache = BaseService._data_cache
        self.folder = cache['folder']
        self.fb = cache['fb']
        self.fb_ref_list = cache['fb_ref_list']
        self.fb_ref_str = cache['fb_ref_str']
        self.drp_str = cache['drp_str']
        self.clinic = cache['clinic']
        self.clinic_list = cache['clinic_list']
        self.clinic_keyword = cache['clinic_keyword']
        self.clinic_complete_type = cache['clinic_complete_type']
        self.stop_words = cache['stop_words']
        self.forward_words = cache['forward_words']
        self.forward_suggestion = cache['forward_suggestion']
        self.clinic_provetcloud = cache['clinic_provetcloud']
        self.msg_provetcloud_reg = cache['msg_provetcloud_reg']
        self.clinic_provetcloud_reg = cache['clinic_provetcloud_reg']
        self.receiver_provetcloud_reg = cache['receiver_provetcloud_reg']
        self.receiver_mappings = cache['receiver_mappings']
        self.number_reg_list = cache['number_reg_list']
        self.attach_reg_list = cache['attach_reg_list']
        self.queries = cache['queries']
        self.email_spec_query = cache['email_spec_query']
        self.admin_query = cache['admin_query']
        self.update_clinic_email_query = cache['update_clinic_email_query']
        self.errand_info_query = cache['errand_info_query']
        self.model_df = cache['model_df']
        self.category_reg_list = cache['category_reg_list']
        self.forward_format = cache['forward_format']
        self.info_reg = cache['info_reg']
        self.bank_map = cache['bank_map']

