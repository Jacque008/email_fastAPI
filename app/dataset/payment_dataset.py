from __future__ import annotations
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from ..services.payment import PaymentService


@dataclass
class PaymentDataset:
    """Dataset class for payment matching - follows DataFrame-first pattern like EmailDataset"""
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    services: Optional[PaymentService] = field(default=None)

    def __post_init__(self):
        """Initialize services after dataclass creation"""
        if self.services is None:
            self.services = PaymentService(self.df)

    def do_match(self) -> pd.DataFrame:
        """
        Perform payment matching on internal DataFrame using fluent API

        Returns:
            DataFrame with matching results
        """
        if self.df.empty:
            return pd.DataFrame()

        if self.services is None:
            self.services = PaymentService(self.df)

        # Load database data first
        errand, payout = self.services.load_preprocess_database()

        # Process using full fluent API chain
        self.services.payment_df = self.df
        processed_service = (self.services
                           .init_payment()
                           .parse_info())

        # Chain errand matching if available
        if not errand.empty:
            processed_service = (processed_service
                               .match_by_info(errand)
                               .reminder_unmatched_amount()
                               .match_entity_and_amount(errand))

        # Chain payout matching if available
        if not payout.empty:
            processed_service = processed_service.match_payout(payout)

        # Get result and format amount
        pay = processed_service.get_result()
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")

        return pay
    
    def get_statistics(self) -> dict:
        """
        Get matching statistics for processed payments DataFrame

        Returns:
            Dictionary with matching statistics
        """
        if self.df.empty:
            return {}

        # Process the internal DataFrame and get statistics
        result_df = self.do_match()
        if self.services is None:
            return {}
        return self.services.statistics(result_df)

    # Removed legacy matching_statistics method - API layer handles Pydantic conversions