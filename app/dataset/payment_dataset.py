from __future__ import annotations
from typing import List
import pandas as pd
from dataclasses import dataclass, field
from ..schemas.payment import PaymentIn, PaymentOut
from ..services.payment import PaymentService
from ..services.utils import model_to_dataframe, dataframe_to_model


@dataclass
class PaymentDataset:
    """Dataset class that chains all atomic functions for payment matching - optimized"""
    services: PaymentService = field(default_factory=PaymentService)

    def match_payments(self, payments: List[PaymentIn]) -> List[PaymentOut]:
        """
        Optimized payment matching using vectorized operations
        
        Args:
            payments: List of PaymentIn objects to process
            
        Returns:
            List of PaymentOut objects with matching results
        """
        if not payments:
            return []

        pay_df = model_to_dataframe(payments)
        result_df = self._chain_process(pay_df)
        
        return dataframe_to_model(result_df, PaymentOut)
    

    def _chain_process(self, pay_df: pd.DataFrame) -> pd.DataFrame:
        """Chain all atomic functions following original Flask logic with performance optimizations"""

        pay = self.services.init_payment(pay_df.copy())
        pay = self.services.parse_info(pay)
        
        errand, payout = self.services.load_preprocess_database()
        if not errand.empty:
            pay = self.services.match_by_info(pay, errand)
            pay = self.services.reminder_unmatched_amount(pay)
            pay = self.services.match_entity_and_amount(pay, errand)
        if not payout.empty:
            pay = self.services.match_payout(pay, payout)

        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        
        return pay
    
    
    def matching_statistics(self, payments: List[PaymentIn]) -> dict:
        """
        Get matching statistics for a set of payments - optimized
        
        Args:
            payments: List of PaymentIn objects to analyze
            
        Returns:
            Dictionary with matching statistics
        """
        if not payments:
            return {}
            
        # Process payments once and get results
        pay_df = model_to_dataframe(payments)
        result_df = self._chain_process(pay_df)
        
        return self.services.statistics(result_df)