from typing import List
import pandas as pd
from dataclasses import dataclass, field
from ..schemas.payment import PaymentIn, PaymentOut
from ..services.payment_service import PaymentService
from ..services.utils import fetchFromDB, model_to_dataframe, dataframe_to_model, tz_convert


@dataclass
class PaymentDataset:
    """Dataset class that chains all atomic functions for payment matching - optimized"""
    service: PaymentService = field(default_factory=PaymentService)

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
        
        # Convert PaymentIn objects to DataFrame
        pay_df = model_to_dataframe(payments)
        
        # Chain the original atomic functions following Flask logic
        result_df = self._chain_process(pay_df)
        
        # Convert back to PaymentOut objects
        return dataframe_to_model(result_df, PaymentOut)
    
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
        
        return self.service.statistics(result_df)
    
    # def _payments_to_dataframe(self, payments: List[PaymentIn]) -> pd.DataFrame:
    #     """Convert list of PaymentIn objects to pandas DataFrame"""
    #     payment_dicts = []
    #     for payment in payments:
    #         payment_dict = payment.model_dump()
    #         payment_dicts.append(payment_dict)
        
    #     return pd.DataFrame(payment_dicts)
    
    def _chain_process(self, pay_df: pd.DataFrame) -> pd.DataFrame:
        """Chain all atomic functions following original Flask logic with performance optimizations"""
        import time
        start_time = time.time()
        
        print(f"🚀 Starting payment processing for {len(pay_df)} payments...")
        
        # Pre-load all data once for better performance
        print("📊 Pre-loading database data...")
        db_start = time.time()
        errand, errand_lookup, payout = self.service.load_preprocess_database()
        db_time = time.time() - db_start
        print(f"✅ Database loaded in {db_time:.2f}s")
        
        # Step 1: Process payments (vectorized)
        step1_start = time.time()
        pay = self.service.init_payment(pay_df.copy())
        print(f"✅ Step 1 (Process): {time.time() - step1_start:.2f}s")
        
        # Step 2: Parse info (keep original logic but add timing)
        step2_start = time.time()
        pay = self.service.parse_info(pay)
        print(f"✅ Step 2 (Parse info): {time.time() - step2_start:.2f}s")
        
        # Step 3: Match by info (optimized but same logic)
        if not errand.empty:
            step3_start = time.time()
            pay = self.service.match_by_info(pay, errand, errand_lookup)
            print(f"✅ Step 3 (Match by info): {time.time() - step3_start:.2f}s")
            print("\n========= after match_by_info ============\n", pay.iloc[0])
            
            # Step 4: Handle unmatched amounts (RESTORED)
            step4_start = time.time()
            pay = self.service.reminder_unmatched_amount(pay)
            print(f"✅ Step 4 (Reminder unmatched): {time.time() - step4_start:.2f}s")
            print("\n========= after reminder_unmatched_amount ============\n", pay.iloc[0])
            # Step 5: Match by entity and amount (RESTORED)
            step5_start = time.time()
            pay = self.service.match_entity_and_amount(pay, errand)
            print(f"✅ Step 5 (Match entity/amount): {time.time() - step5_start:.2f}s")
        print("\n========= after match_entity_and_amount ============\n", pay.iloc[0])
        # Step 6: Match payouts
        if not payout.empty:
            step6_start = time.time()
            pay = self.service.match_payout(pay, payout)
            print(f"✅ Step 6 (Match payouts): {time.time() - step6_start:.2f}s")
            print("\n========= after match_payout ============\n", pay.iloc[0])
        # Format amount for display (vectorized)
        format_start = time.time()
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        print(f"✅ Format amounts: {time.time() - format_start:.2f}s")
        print("\n========= after Format amounts ============\n", pay.iloc[0])
        total_time = time.time() - start_time
        print(f"\n⏱️ Total processing time: {total_time:.2f}s ({len(pay_df)/total_time:.1f} payments/sec)")
        
        return pay
    

    # def _dataframe_to_payments(self, df: pd.DataFrame) -> List[PaymentOut]:
    #     """Convert pandas DataFrame to list of PaymentOut objects"""
    #     payments = []
        
    #     for _, row in df.iterrows():
    #         payment = PaymentOut(
    #             id=row['id'],
    #             reference=row.get('reference'),
    #             bankName=row['bankName'],
    #             amount=row['amount'],  # Already formatted as "X.XX kr"
    #             info=row.get('info'),
    #             createdAt=row['createdAt'],
    #             insuranceCaseId=row.get('insuranceCaseId', []),
    #             status=row.get('status', ''),
    #             val_pay=row.get('val_pay', []),
    #             valErrand=row.get('valErrand', []),
    #             isReference=row.get('isReference', []),
    #             referenceLink=row.get('referenceLink', []),
    #             settlementAmount=row.get('settlementAmount', 0.0),
    #             extractReference=row.get('extractReference'),
    #             extractDamageNumber=row.get('extractDamageNumber'),
    #             extractOtherNumber=row.get('extractOtherNumber')
    #         )
    #         payments.append(payment)
            
    #     return payments
    