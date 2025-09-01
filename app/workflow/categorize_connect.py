from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
from .preprocess import PreProcess
from ..services.services import DefaultServices

@dataclass
class CategoryConnect:
    df: pd.DataFrame
    services: DefaultServices = field(default_factory=DefaultServices)

    def __post_init__(self):
        self.connector = self.services.get_connector()
        self.classifier = self.services.get_classifier()
        self.extractor = self.services.get_extractor()
        
    def do_connect(self) -> pd.DataFrame:
        """Categorize emails and connect them with errands."""
        pp =  PreProcess(self.services) 
        self.df = pp.do_preprocess(self.df)
        self.df = self.classifier.initialize_columns(self.df)
        self.df = self.extractor.extract_numbers_from_attach(self.df)
        self.df = self.extractor.extract_numbers_from_email(self.df)
        self.df = self.classifier.categorize_emails(self.df)
        self.df = self.connector.connect_with_time_windows(self.df)
        self.df = self.classifier.refine_finalize(self.df)

        return self.df


