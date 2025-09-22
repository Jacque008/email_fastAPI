from __future__ import annotations
import pandas as pd
import time
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
        start_time = time.time()

        step_start = time.time()
        self.processor = self.services.get_processor()

        step_start = time.time()
        self.parser = self.services.get_parser()

        step_start = time.time()
        self.sender_detector = self.services.get_sender_detector()

        step_start = time.time()
        self.receiver_detector = self.services.get_receiver_detector()

        step_start = time.time()
        self.staff_detector = self.services.get_staff_detector()

        step_start = time.time()
        self.extractor = self.services.get_extractor()

        step_start = time.time()
        self.classifier = self.services.get_classifier()

        step_start = time.time()
        self.connector = self.services.get_connector()

        total_time = time.time() - start_time
    
    def adjust_time(self) -> "EmailDataset":
        self.df = self.processor.adjust_time_format(self.df)
        return self
    
    def generate_content(self) -> "EmailDataset":
        self.df = self.processor.generate_email_content(self.df)
        return self

    def detect_sender(self) -> "EmailDataset":
        self.df = self.sender_detector.detect_sender(self.df)
        return self

    def detect_receiver(self) -> "EmailDataset":
        self.df = self.receiver_detector.detect_receiver(self.df)
        return self

    def handle_vendor_specials(self) -> "EmailDataset":
        """Applies parser vendor-specific fixes (Provet Cloud, Wisentic)."""
        self.df = self.parser.handle_provet_cloud(self.df)
        self.df = self.parser.handle_wisentic(self.df)
        if 'reference' in self.df.columns:
            self.df['insuranceCaseRef'] = self.df['reference']
        
        return self

    # --- Extraction --------------------------------------------------------
    def extract_attachments(self) -> "EmailDataset":
        self.df = self.extractor.extract_numbers_from_attach(self.df)
        return self
    
    def extract_emails(self) -> "EmailDataset":
        self.df = self.extractor.extract_numbers_from_email(self.df)
        return self


    def refine_finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.classifier.refine_finalize(self.df)
    
    
    # --- Utilities ---------------------------------------------------------
    def sort_by_date(self, ascending: bool = True) -> "EmailDataset":
        if 'date' in self.df.columns:
            self.df = self.df.sort_values('date', ascending=ascending)
        return self

    # Convenience: expose the underlying DataFrame explicitly
    def to_frame(self) -> pd.DataFrame:
        return self.df
    
    def process_emails(self) -> "EmailDataset":
        """
        Preprocess emails: adjust_time_format -> detect_sender -> generate_email_content
        -> detect_receiver -> return
        """
        (self
            .adjust_time()              
            .detect_sender()
            .generate_content()  
            .detect_receiver()
        )     

        return self
    
    def do_preprocess(self) -> pd.DataFrame:
        """
        Preprocess emails: adjust_time_format -> detect_sender -> generate_email_content
        -> detect_receiver -> vendor specials -> sort -> filter columns -> return
        """
        (self.process_emails()
            .handle_vendor_specials()
            .sort_by_date(ascending=True))
        
        # Ensure all required columns exist
        for col in self.RETURN_COLS:
            if col not in self.df.columns:
                self.df[col] = None        
        
        # Filter to only return required columns
        return self.df[list(self.RETURN_COLS)]
    
    def do_connect(self) -> pd.DataFrame:
        """Categorize emails and connect them with errands."""
        try:
            start_time = time.time()

            step_start = time.time()
            self.df = self.do_preprocess()

            step_start = time.time()
            self.df = self.classifier.initialize_columns(self.df)

            step_start = time.time()
            self.df = self.extractor.extract_numbers_from_attach(self.df)

            step_start = time.time()
            self.df = self.extractor.extract_numbers_from_email(self.df)

            step_start = time.time()
            self.df = self.classifier.categorize_emails(self.df)

            step_start = time.time()
            self.df = self.connector.connect_with_time_windows(self.df)

            step_start = time.time()
            self.df = self.classifier.refine_finalize(self.df)

            total_time = time.time() - start_time

            return self.df

        except Exception as e:
            import traceback
            print(f"❌ do_connect: Error occurred - {str(e)}")
            print(f"❌ Full traceback: {traceback.format_exc()}")
            raise e

