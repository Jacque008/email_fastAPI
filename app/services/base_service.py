import os
import regex as reg
import pandas as pd
from dotenv import load_dotenv
import time
import signal
load_dotenv()

# Set up GCS authentication based on environment
env_mode = os.getenv('ENV_MODE', 'local')
if env_mode == 'local':
    service_account_file = "data/other/drp-system-73cd3f0ca038.json"
    if os.path.exists(service_account_file):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file
        print(f"🔐 Using local service account credentials: {service_account_file}")
elif env_mode in ['test', 'production']:
    service_account_file = "/SERVICE_ACCOUNT_JIE/SERVICE_ACCOUNT_JIE"
    if os.path.exists(service_account_file):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file
        print(f"🔐 Using cloud secret service account credentials: {service_account_file}")
    else:
        print(f"⚠️  Cloud secret not found at: {service_account_file}")

class BaseService:
    """
    Base service class for all business services.
    Provides common functionality like data loading and environment configuration.
    """
    def __init__(self):
        print("⚙️ Initializing BaseService...")
        start_time = time.time()

        # Always use local data folder since CSV files are now copied to Docker image
        self.folder = "data/para_tables"
        print(f"📁 Using folder: {self.folder}")

        # Always try to load from local files (whether local dev or cloud)
        try:
            self._load_tables()
            elapsed = time.time() - start_time
            print(f"✅ BaseService initialized successfully in {elapsed:.2f}s")

        except Exception as e:
            print(f"❌ BaseService initialization failed: {e}")
            # Fall back to defaults only if file loading fails
            self._set_defaults()
            elapsed = time.time() - start_time
            print(f"✅ BaseService initialized with defaults after error in {elapsed:.2f}s")

    def _timeout_handler(self, signum, frame):
        raise TimeoutError("GCS loading timed out")

    def _set_defaults(self):
        """Set default empty values to prevent crashes"""
        self.fb_ref_list = ['lassie', 'agria', 'sveland', 'folksam', 'trygg', 'moderna', 'dina', 'hedvig', 'ica']
        self.fb_ref_str = 'lassie|agria|sveland|folksam|trygg|moderna|dina|hedvig|ica'
        self.drp_str = 'direktregleringsportalen'
        self.fb = pd.DataFrame(columns=['insuranceCompanyReference'])
        self.clinic = pd.DataFrame(columns=['clinicId', 'clinicName', 'clinicEmail', 'keyword', 'provetCloud'])
        self.clinic_list = pd.DataFrame()
        self.clinic_keyword = pd.DataFrame(columns=['clinicName', 'keyword'])
        self.clinic_complete_type = ["direktreglering", "skadeanmälan"]
        self.stop_words = ["Original message", "Från:", "From:", "Skickat:"]
        self.forward_words = ["Vidarebefordrat meddelande", "Forwarded message", "FW:", "Fwd:"]
        self.forward_suggestion = pd.DataFrame(columns=['action', 'templates'])
        self.clinic_provetcloud = pd.DataFrame(columns=['clinicName', 'provetCloud', 'keyword'])
        self.msg_provetcloud_reg = []
        self.clinic_provetcloud_reg = []
        self.receiver_provetcloud_reg = []
        self.receiver_mappings = {
            'sveland': 'Sveland',
            'agria': 'Agria',
            'dina': 'Dina Försäkringar',
            'trygg': 'Trygg-Hansa',
            'moderna': 'Moderna Försäkringar',
            'ica': 'ICA Försäkring',
            'hedvig': 'Hedvig',
            'lassie': 'Lassie',
            'folksam': 'Folksam'
        }
        # Add essential regex patterns that the code depends on
        essential_number_patterns = [
            {'number': 'reference', 'regex': r'(?:Referens|Reference|Ref)\s*:?\s*([A-Z0-9-]+)'},
            {'number': 'insuranceNumber', 'regex': r'(?:Försäkringsnummer|Insurance\s*number)\s*:?\s*([A-Z0-9-]+)'},
            {'number': 'damageNumber', 'regex': r'(?:Skadenummer|Damage\s*number)\s*:?\s*([A-Z0-9-]+)'},
            {'number': 'settlementAmount', 'regex': r'(?:Utbetalt\s*belopp|Settlement\s*amount)\s*:?\s*([0-9\s,.-]+)\s*kr'},
            {'number': 'totalAmount', 'regex': r'(?:Total|Totalt|Kostnad)\s*:?\s*([0-9\s,.-]+)\s*kr'},
            {'number': 'animalName', 'regex': r'(?:Djurets?\s*namn|Animal\s*name)\s*:?\s*([A-Za-zåäöÅÄÖ\s-]+)'},
            {'number': 'ownerName', 'regex': r'(?:Djurägare|Ägarens?\s*namn|Owner)\s*:?\s*([A-Za-zåäöÅÄÖ\s-]+)'}
        ]
        self.number_reg_list = pd.DataFrame(essential_number_patterns)

        essential_attach_patterns = [
            {'regex': r'(?:Utbetalt\s*belopp|Settlement)\s*:?\s*([0-9\s,.-]+)\s*kr'},
            {'regex': r'(?:Total|Kostnad)\s*:?\s*([0-9\s,.-]+)\s*kr'}
        ]
        self.attach_reg_list = pd.DataFrame(essential_attach_patterns)
        self.queries = pd.DataFrame(columns=[
            'emailSpec', 'admin', 'updateClinicEmail', 'errandInfo', 'info',
            'errandPay', 'errandLink', 'payout', 'summaryChat', 'summaryEmail',
            'summaryComment', 'logBase', 'logEmail', 'logChat', 'logComment',
            'logOriginalInvoice', 'logInvoiceSP', 'logInvoiceFO', 'logInvoiceKA',
            'logReceive', 'logCancel', 'logRemoveCancel', 'errandConnect'
        ])
        self.email_spec_query = ""
        self.admin_query = ""
        self.update_clinic_email_query = ""
        self.errand_info_query = ""
        self.info_query = ""
        self.errand_pay_query = ""
        self.errand_link_query = ""
        self.payout_query = ""
        self.model_df = pd.DataFrame(columns=['model'])

    def _load_tables(self):
        """Load all required data tables and configuration files"""
        print(f"Loading tables from: {self.folder}")

        # Helper function to safely load CSV
        def safe_load_csv(filename):
            try:
                print(f"📁 Loading {filename}...")
                start = time.time()

                # Read the CSV file - read ALL columns
                df = pd.read_csv(f"{self.folder}/{filename}")

                elapsed = time.time() - start
                print(f"✅ Successfully loaded {filename} ({len(df)} rows) in {elapsed:.2f}s")
                return df
            except Exception as e:
                print(f"❌ Error loading {filename}: {e}")
                return pd.DataFrame()  # Return empty DataFrame if file not found

        # Insurance company data
        self.fb = safe_load_csv("fb.csv")

        # Process fb data safely
        try:
            if not self.fb.empty and 'insuranceCompanyReference' in self.fb.columns:
                self.fb_ref_list = [str(fb) for fb in self.fb[:-5].insuranceCompanyReference.tolist()]
                self.fb_ref_str = '|'.join([reg.escape(fb) for fb in self.fb_ref_list])
                drp_list = [str(fb) for fb in self.fb[-5:].insuranceCompanyReference.tolist()]
                self.drp_str = '|'.join([reg.escape(fb) for fb in drp_list])
            else:
                self.fb_ref_list = []
                self.fb_ref_str = ""
                self.drp_str = ""
        except Exception as e:
            print(f"Error processing fb data: {e}")
            self.fb_ref_list = []
            self.fb_ref_str = ""
            self.drp_str = ""

        # Clinic data
        self.clinic = safe_load_csv("clinic.csv")
        self.clinic_list = self.clinic[['clinicId','clinicName','clinicEmail']].drop_duplicates(subset=['clinicEmail'], keep='last') if not self.clinic.empty else pd.DataFrame()

        if not self.clinic.empty:
            self.clinic_keyword = self.clinic[self.clinic['keyword'].notna()][['clinicName','keyword']].drop_duplicates()
            self.clinic_keyword['keyword'] = self.clinic_keyword['keyword'].apply(lambda x: x.split(',') if isinstance(x, str) else [])
        else:
            self.clinic_keyword = pd.DataFrame(columns=['clinicName', 'keyword'])

        # Other CSV files
        clinic_comp_df = safe_load_csv("clinicCompType.csv")
        self.clinic_complete_type = clinic_comp_df['complement'].tolist() if 'complement' in clinic_comp_df.columns else []

        # Text processing data
        stop_words_df = safe_load_csv("stopWords.csv")
        self.stop_words = stop_words_df['stopWords'].tolist() if 'stopWords' in stop_words_df.columns else []

        forward_words_df = safe_load_csv("forwardWords.csv")
        self.forward_words = forward_words_df['forwardWords'].tolist() if 'forwardWords' in forward_words_df.columns else []

        self.forward_suggestion = safe_load_csv("forwardSuggestion.csv")

        # Provet Cloud specific data
        if not self.clinic.empty:
            self.clinic_provetcloud = self.clinic[self.clinic['provetCloud'].notna()][['clinicName','provetCloud']].drop_duplicates()
            self.clinic_provetcloud['keyword'] = self.clinic_provetcloud['provetCloud'].apply(lambda x: x.split(',') if isinstance(x, str) else [])
        else:
            self.clinic_provetcloud = pd.DataFrame(columns=['clinicName', 'provetCloud', 'keyword'])

        # Safe extraction from forward_suggestion
        try:
            self.msg_provetcloud_reg = self.forward_suggestion[self.forward_suggestion['action']=='ProvetCloud_Msg']['templates'].tolist() if 'templates' in self.forward_suggestion.columns else []
            self.clinic_provetcloud_reg = self.forward_suggestion[self.forward_suggestion['action']=='ProvetCloud_Clinic']['templates'].tolist() if 'templates' in self.forward_suggestion.columns else []
            self.receiver_provetcloud_reg = self.forward_suggestion[self.forward_suggestion['action']=='ProvetCloud_Recipient']['templates'].tolist() if 'templates' in self.forward_suggestion.columns else []
        except Exception as e:
            print(f"Error processing ProvetCloud data: {e}")
            self.msg_provetcloud_reg = []
            self.clinic_provetcloud_reg = []
            self.receiver_provetcloud_reg = []

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
        self.number_reg_list = safe_load_csv("numberReg.csv")
        self.attach_reg_list = safe_load_csv("attachReg.csv")

        # Database queries
        self.queries = safe_load_csv("queries.csv")
        try:
            self.email_spec_query = self.queries['emailSpec'].iloc[0] if 'emailSpec' in self.queries.columns and not self.queries.empty else ""
            self.admin_query = self.queries['admin'].iloc[0] if 'admin' in self.queries.columns and not self.queries.empty else ""
            self.update_clinic_email_query = self.queries['updateClinicEmail'].iloc[0] if 'updateClinicEmail' in self.queries.columns and not self.queries.empty else ""
            self.errand_info_query = self.queries['errandInfo'].iloc[0] if 'errandInfo' in self.queries.columns and not self.queries.empty else ""
            self.info_query = self.queries['info'].iloc[0] if 'info' in self.queries.columns and not self.queries.empty else ""
            self.errand_pay_query = self.queries['errandPay'].iloc[0] if 'errandPay' in self.queries.columns and not self.queries.empty else ""
            self.errand_link_query = self.queries['errandLink'].iloc[0] if 'errandLink' in self.queries.columns and not self.queries.empty else ""
            self.payout_query = self.queries['payout'].iloc[0] if 'payout' in self.queries.columns and not self.queries.empty else ""
        except Exception as e:
            print(f"Error processing queries: {e}")
            self.email_spec_query = self.admin_query = self.update_clinic_email_query = self.errand_info_query = ""
            self.info_query = self.errand_pay_query = self.errand_link_query = self.payout_query = ""

        self.model_df = safe_load_csv("model.csv")

        print("BaseService initialization completed successfully")

