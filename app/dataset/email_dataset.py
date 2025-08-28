from __future__ import annotations
import pandas as pd
from dataclasses import dataclass, field
from ..services.services import DefaultServices

@dataclass
class EmailDataset:
    """Encapsulates the incoming emails DataFrame and exposes
    object-oriented operations that mutate internal state and return `self`,
    so you can fluently chain method calls.
    """
    df: pd.DataFrame
    services: DefaultServices = field(default_factory=DefaultServices)

    def __post_init__(self):
        self.processor = self.services.get_processor()
        self.parser = self.services.get_parser()
        self.sender_detector = self.services.get_sender_detector()
        self.receiver_detector = self.services.get_receiver_detector()
        self.staff_detector = self.services.get_staff_detector()
        self.extractor = self.services.get_extractor()
        self.classifier = self.services.get_classifier()
    
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

    # --- Categorization ----------------------------------------------------
    def initialize_categorize(self) -> "EmailDataset":
        self.df = self.classifier.initialize_columns(self.df)
        self.df = self.classifier.categorize_emails(self.df)
        return self

    def refine_finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        self.df = self.classifier.refine_categories(df)
        return self.classifier.finalize_columns(self.df)
    
    # --- Staff-animal enrichment (optional) --------------------------------
    def enrich_staff_animal(self) -> "EmailDataset":
        self.df = self.staff_detector.detect_staff_animals(self.df)
        return self

    def finalize(self) -> pd.DataFrame:
        """Finalizes the DataFrame by applying all necessary transformations."""
        self.df = (self.classifier.process_fbReference(self.df)
                                .pipe(self.classifier.process_info)
                                .pipe(self.classifier.process_show_page)
                                .pipe(self.classifier.process_icReference)
                                .pipe(self.enrich_staff_animal)
        )
        final_cols = [
            'id', 'from', 'sender', 'source', 'to', 'receiver', 
            'insuranceCompanyReference', 'category', 'errandId', 
            'totalAmount', 'settlementAmount', 'reference', 
            'insuranceCaseRef', 'insuranceNumber', 'damageNumber', 
            'animalName', 'ownerName', 'note', 'showPage', 'isStaffAnimal']
        
        return self.df[[col for col in final_cols if col in self.df.columns]]

    # --- Utilities ---------------------------------------------------------
    def sort_by_date(self, ascending: bool = True) -> "EmailDataset":
        if 'date' in self.df.columns:
            self.df = self.df.sort_values('date', ascending=ascending)
        return self

    # Convenience: expose the underlying DataFrame explicitly
    def to_frame(self) -> pd.DataFrame:
        return self.df

