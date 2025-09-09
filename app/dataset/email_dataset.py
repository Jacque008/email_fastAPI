from __future__ import annotations
import pandas as pd
from dataclasses import dataclass, field
from typing import Sequence
from ..services.services import DefaultServices

@dataclass
class EmailDataset:
    """Encapsulates the incoming emails DataFrame and exposes
    object-oriented operations that mutate internal state and return `self`,
    so you can fluently chain method calls.
    """
    df: pd.DataFrame
    services: DefaultServices = field(default_factory=DefaultServices)
    
    # Standard return columns for preprocessing
    RETURN_COLS: Sequence[str] = (
        'id','date','from','originSender','sender','source',
        'to','originReceiver','receiver','sendTo','clinicCompType',
        'reference','insuranceCaseRef','errandId','category',
        'subject','origin','email','attachments','textHtml'
    )

    def __post_init__(self):
        self.processor = self.services.get_processor()
        self.parser = self.services.get_parser()
        self.sender_detector = self.services.get_sender_detector()
        self.receiver_detector = self.services.get_receiver_detector()
        self.staff_detector = self.services.get_staff_detector()
        self.extractor = self.services.get_extractor()
        self.classifier = self.services.get_classifier()
        self.connector = self.services.get_connector()
    
    def adjust_time(self) -> "EmailDataset":
        # print("********debug in adjust_time before : ", self.df.shape, self.df.columns)
        self.df = self.processor.adjust_time_format(self.df)
        # print(  "=======debug in adjust_time after : ", self.df.shape, self.df.columns)
        return self
    
    def generate_content(self) -> "EmailDataset":
        # print("********debug in generate_content before : ", self.df.shape, self.df.columns)
        self.df = self.processor.generate_email_content(self.df)
        # print("=======debug in generate_content after : ", self.df.shape, self.df.columns)
        return self

    def detect_sender(self) -> "EmailDataset":
        # print("********debug in detect_sender before : ", self.df.shape, self.df.columns)
        self.df = self.sender_detector.detect_sender(self.df)
        # print("=======debug in detect_sender after : ", self.df.shape, self.df.columns)
        return self

    def detect_receiver(self) -> "EmailDataset":
        # print("********debug in detect_receiver before : ", self.df.shape, self.df.columns)
        self.df = self.receiver_detector.detect_receiver(self.df)
        # print("=======debug in detect_receiver after : ", self.df.shape, self.df.columns)
        return self

    def handle_vendor_specials(self) -> "EmailDataset":
        """Applies parser vendor-specific fixes (Provet Cloud, Wisentic)."""
        # print("********debug in handle_vendor_specials before : ", self.df.columns)
        self.df = self.parser.handle_provet_cloud(self.df)
        self.df = self.parser.handle_wisentic(self.df)
        if 'reference' in self.df.columns:
            self.df['insuranceCaseRef'] = self.df['reference']
        # print("=======debug in handle_vendor_specials after : ", self.df.columns)
        
        return self

    # --- Extraction --------------------------------------------------------
    def extract_attachments(self) -> "EmailDataset":
        # print("********debug in extract_attachments before : ", self.df.shape, self.df.columns)
        self.df = self.extractor.extract_numbers_from_attach(self.df)
        # print("=======debug in extract_attachments after : ", self.df.shape, self.df.columns)
        return self
    
    def extract_emails(self) -> "EmailDataset":
        # print("********debug in extract_emails before : ", self.df.shape, self.df.columns)
        self.df = self.extractor.extract_numbers_from_email(self.df)
        # print("=======debug in adextract_emailsjust_time after : ", self.df.shape, self.df.columns)
        return self


    def refine_finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # print("********debug in refine_finalize before : ", self.df.shape, self.df.columns)
        # self.df = self.classifier.refine_categories(df)
        # print("=======debug in refine_finalize after : ", self.df.shape, self.df.columns)
        return self.classifier.refine_finalize(self.df)
    
    
    # --- Utilities ---------------------------------------------------------
    def sort_by_date(self, ascending: bool = True) -> "EmailDataset":
        # print("********debug in sort_by_date before : ", self.df.shape, self.df.columns)
        if 'date' in self.df.columns:
            self.df = self.df.sort_values('date', ascending=ascending)
        # print("=======debug in sort_by_date after : ", self.df.shape, self.df.columns)
        return self

    # Convenience: expose the underlying DataFrame explicitly
    def to_frame(self) -> pd.DataFrame:
        return self.df
    
    def do_preprocess(self) -> pd.DataFrame:
        """
        Preprocess emails: adjust_time_format -> detect_sender -> generate_email_content
        -> detect_receiver -> vendor specials -> sort -> filter columns -> return
        """
        (self
            .adjust_time()              
            .detect_sender()
            .generate_content()  
            .detect_receiver()
            .handle_vendor_specials()
            .sort_by_date(ascending=True)
        )
        
        # Ensure all required columns exist
        for col in self.RETURN_COLS:
            if col not in self.df.columns:
                self.df[col] = None        
        
        # Filter to only return required columns
        return self.df[list(self.RETURN_COLS)]
    
    def do_connect(self) -> pd.DataFrame:
        """Categorize emails and connect them with errands."""
        try:
            self.df = self.do_preprocess()
            self.df = self.classifier.initialize_columns(self.df)
            self.df = self.extractor.extract_numbers_from_attach(self.df)
            self.df = self.extractor.extract_numbers_from_email(self.df)
            self.df = self.classifier.categorize_emails(self.df)
            self.df = self.connector.connect_with_time_windows(self.df)
            self.df = self.classifier.refine_finalize(self.df)
            
            return self.df
            
        except Exception as e:
            print(f"Error in do_connect at step: {str(e)}")
            raise e

