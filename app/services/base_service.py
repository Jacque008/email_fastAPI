import os
import regex as reg
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

class BaseService:
    """
    Base service class for all business services.
    Provides common functionality like data loading and environment configuration.
    """
    def __init__(self):
        env = os.getenv('ENV_MODE')
        self.folder = {
            'local': "data/para_tables",
            'test': "gs://ml_email_test/fastapi",
            'production': "gs://ml_email_category/fastapi"
        }.get(env, "data/para_tables")
        print("✅ ******** self.folde: ", self.folder)
        self._load_tables()

    def _load_tables(self):
        """Load all required data tables and configuration files"""
        # Insurance company data
        self.fb = pd.read_csv(f"{self.folder}/fb.csv")
        print("✅ ******** self.fb: ", self.fb.shape)
        self.fb_ref_list = self.fb[:-5].insuranceCompanyReference.tolist()
        print("✅ ******** self.fb_ref_list: ", self.fb_ref_list)
        self.fb_ref_str = '|'.join([reg.escape(fb) for fb in self.fb_ref_list])
        self.drp_str = '|'.join([reg.escape(fb) for fb in self.fb[-5:].insuranceCompanyReference.tolist()])
        
        # Clinic data
        self.clinic = pd.read_csv(f"{self.folder}/clinic.csv")
        print("✅ ******** self.clinic: ", self.clinic.shape)
        self.clinic_list = self.clinic[['clinicId','clinicName','clinicEmail']].drop_duplicates(subset=['clinicEmail'], keep='last')
        self.clinic_keyword = self.clinic[self.clinic['keyword'].notna()][['clinicName','keyword']].drop_duplicates()
        self.clinic_keyword['keyword'] = self.clinic_keyword['keyword'].apply(lambda x: x.split(',') if isinstance(x, str) else [])
        self.clinic_complete_type = pd.read_csv(f"{self.folder}/clinicCompType.csv").complement.tolist()

        # Text processing data
        self.stop_words = pd.read_csv(f"{self.folder}/stopWords.csv").stopWords.tolist()
        print("✅ ******** self.stop_words: ", self.stop_words[:4])
        self.forward_words = pd.read_csv(f"{self.folder}/forwardWords.csv").forwardWords.tolist()
        self.forward_suggestion = pd.read_csv(f"{self.folder}/forwardSuggestion.csv")
        print("✅ ******** self.forward_suggestion: ", self.forward_suggestion.shape)
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
        print("✅ ******** self.queries: ", self.queries.shape)
        self.email_spec_query = self.queries['emailSpec'].iloc[0]
        self.admin_query = self.queries['admin'].iloc[0]
        self.update_clinic_email_query = self.queries['updateClinicEmail'].iloc[0]
        self.errand_info_query = self.queries['errandInfo'].iloc[0]
        self.model_df = pd.read_csv(f"{self.folder}/model.csv")

