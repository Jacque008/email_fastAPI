from __future__ import annotations
import pandas as pd
from dataclasses import dataclass, field
from typing import Sequence
from ..services.services import DefaultServices
from ..services.processor import Processor
from ..services.parser import Parser
from ..services.resolver import SenderResolver, ReceiverResolver, StaffResolver
from ..services.extractor import Extractor
from ..services.classifier import Classifier
from ..services.connector import Connector



@dataclass
class EmailDataset:
    """Encapsulates the incoming emails DataFrame and exposes
    object-oriented operations that mutate internal state and return `self`,
    so you can fluently chain method calls.
    """
    df: pd.DataFrame
    services: DefaultServices = field(default_factory=DefaultServices)

    # Services initialized in __post_init__
    processor: Processor = field(init=False)
    parser: Parser = field(init=False)
    sender_detector: SenderResolver = field(init=False)
    receiver_detector: ReceiverResolver = field(init=False)
    staff_detector: StaffResolver = field(init=False)
    extractor: Extractor = field(init=False)
    classifier: Classifier = field(init=False)
    connector: Connector = field(init=False)
    
    # Standard return columns for preprocessing
    RETURN_COLS: Sequence[str] = (
        'id','date','from','originSender','sender','source',
        'to','originReceiver','receiver','sendTo','clinicCompType',
        'reference','insuranceCaseRef','errandId','category',
        'subject','origin','email','attachments','textHtml'
    )

    def __post_init__(self):
        """Initialize all services after dataclass creation"""
        self.processor = self.services.get_processor()
        self.parser = self.services.get_parser()
        self.sender_detector = self.services.get_sender_detector()
        self.receiver_detector = self.services.get_receiver_detector()
        self.staff_detector = self.services.get_staff_detector()
        self.extractor = self.services.get_extractor()
        self.classifier = self.services.get_classifier()
        self.connector = self.services.get_connector()
    
    # All wrapper methods and FluentService removed - use direct service calls
    # Example: ds.processor.adjust_time_format(df)


    def refine_finalize(self) -> pd.DataFrame:
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
        self.df = self.processor.adjust_time_format(self.df)
        self.df = self.sender_detector.detect_sender(self.df)
        self.df = self.processor.generate_email_content(self.df)
        self.df = self.receiver_detector.detect_receiver(self.df)

        return self
    
    def do_preprocess(self) -> pd.DataFrame:
        """
        Preprocess emails: adjust_time_format -> detect_sender -> generate_email_content
        -> detect_receiver -> vendor specials -> sort -> filter columns -> return
        """
        self.process_emails()
        self.df = self.parser.handle_provet_cloud(self.df)
        self.df = self.parser.handle_wisentic(self.df)
        if 'reference' in self.df.columns:
            self.df['insuranceCaseRef'] = self.df['reference']
        self.sort_by_date(ascending=True)

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
            raise Exception(f"do_connect: Error occurred - {str(e)}")

