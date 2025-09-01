"""
Payment Dataset Management
Payment dataset management module
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import regex as reg
from pathlib import Path
import json

from .payment import PaymentIn, PaymentOut, PaymentStatistics
from ..app.services.utils import fetchFromDB


class PaymentDataset:
    def __init__(self, data_path: Optional[str] = None):
        self.data_path = data_path or "data/test_data"
        self.payments = pd.DataFrame()
        self.processed_payments_df = pd.DataFrame()
        self.statistics = {}
        self.base_columns = [
            'id', 'amount', 'reference', 'info', 'bankName', 
            'createdAt', 'updatedAt', 'status'
        ]
        
        self.processed_columns = [
            'id', 'amount', 'reference', 'info', 'bankName', 'createdAt',
            'extractReference', 'extractDamageNumber', 'extractOtherNumber',
            'insuranceCaseId', 'settlementAmount', 'matchStatus', 
            'referenceLinks', 'processingTime', 'errorMessage'
        ]
    
    def load_from_database(self, query: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load payment data from database
        
        Args:
            query: Custom query statement
            limit: Limit result count
            
        Returns:
            pd.DataFrame: Payment data
        """
        if query is None:
            query = """
            SELECT 
                p.id,
                p.amount,
                p.reference,
                p.info,
                p.bank_name as bankName,
                p.created_at as createdAt,
                p.updated_at as updatedAt,
                p.status
            FROM payments p
            WHERE p.deleted_at IS NULL
            """
            
        if limit:
            query += f" LIMIT {limit}"
            
        try:
            self.payments = fetchFromDB(query)
            if not self.payments.empty:
                self.payments['createdAt'] = pd.to_datetime(
                    self.payments['createdAt'], utc=True
                ).dt.tz_convert('Europe/Stockholm')
                self.payments['amount'] = pd.to_numeric(self.payments['amount'], errors='coerce')
                
            return self.payments
        except Exception as e:
            print(f"Failed to load payment data: {str(e)}")
            return pd.DataFrame()
    
    def load_from_file(self, file_path: str, file_format: str = 'json') -> pd.DataFrame:
        """
        Load payment data from file
        
        Args:
            file_path: File path
            file_format: File format ('json', 'csv', 'excel')
            
        Returns:
            pd.DataFrame: Payment data
        """
        try:
            if file_format.lower() == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.payments = pd.DataFrame(data)
            elif file_format.lower() == 'csv':
                self.payments = pd.read_csv(file_path)
            elif file_format.lower() in ['excel', 'xlsx']:
                self.payments = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Data preprocessing
            self._process_payment()
            return self.payments
            
        except Exception as e:
            print(f"Failed to load file data: {str(e)}")
            return pd.DataFrame()
    
    def create_sample_data(self, size: int = 100) -> pd.DataFrame:
        """
        Create sample payment data
        
        Args:
            size: Data count
            
        Returns:
            pd.DataFrame: Sample data
        """
        np.random.seed(42)
        
        # Sample bank names
        bank_names = [
            'Agria Djurförsäkring', 'If Skadeförsäkring', 'Trygg-Hansa', 
            'Länsförsäkringar', 'Folksam', 'SEB', 'Swedbank', 'Nordea'
        ]
        
        # Generate sample data
        sample_data = []
        for i in range(size):
            payment_id = 40000 + i
            amount = np.random.uniform(500, 50000)  # 500-50000 SEK
            reference = f"100{np.random.randint(100000, 999999)}"
            bank = np.random.choice(bank_names)
            
            # Generate info field
            damage_number = np.random.randint(1000000, 9999999)
            invoice_number = f"11{np.random.randint(100000000, 999999999)}"
            
            info = f"""SKADEUTBETALNING
                        SKADENUMMER: {damage_number}
                        FAKTURANUMMER: {invoice_number}
                        SPECIFIKATION ENLIGT DIREKTREGLERING
                        KLINIK: Testdjurklinik AB
                        ÄGARE: Test Testsson
                        DJUR: Hund, Labrador
                        """
            
            days_ago = np.random.randint(1, 365)
            created_at = datetime.now() - timedelta(days=days_ago)
            
            sample_data.append({
                'id': payment_id,
                'amount': amount,
                'reference': reference,
                'info': info,
                'bankName': bank,
                'createdAt': created_at.isoformat(),
                'status': 'pending'
            })
        
        self.payments = pd.DataFrame(sample_data)
        return self.payments
    
    def _process_payment(self):
        if self.payments.empty:
            return
            
        if 'createdAt' in self.payments.columns:
            self.payments['createdAt'] = pd.to_datetime(
                self.payments['createdAt'], errors='coerce', utc=True
            )
        
        if 'amount' in self.payments.columns:
            self.payments['amount'] = pd.to_numeric(
                self.payments['amount'], errors='coerce'
            )
        
        self.payments['reference'] = self.payments['reference'].fillna('')
        self.payments['info'] = self.payments['info'].fillna('')
        self.payments['status'] = self.payments['status'].fillna('pending')
    
    def extract_payment_info(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Extract structured data from payment information
        
        Args:
            df: DataFrame to process, if None process self.payments
            
        Returns:
            pd.DataFrame: Processed data
        """
        if df is None:
            df = self.payments.copy()
        
        if df.empty:
            return df
        
        # Extract reference numbers
        ref_pattern = reg.compile(r'\d+')
        df['extractReference'] = df['reference'].apply(
            lambda x: ''.join(ref_pattern.findall(str(x))) if pd.notna(x) else None
        )
        
        # Extract information from info field
        df['extractDamageNumber'] = df['info'].apply(self._extract_damage_number)
        df['extractInvoiceNumber'] = df['info'].apply(self._extract_invoice_number)
        df['extractClinicName'] = df['info'].apply(self._extract_clinic_name)
        df['extractOwnerName'] = df['info'].apply(self._extract_owner_name)
        df['extractAnimalInfo'] = df['info'].apply(self._extract_animal_info)
        
        return df
    
    def _extract_damage_number(self, info: str) -> Optional[str]:
        """Extract damage number from info"""
        if pd.isna(info):
            return None
        
        patterns = [
            r'SKADENUMMER:\s*([0-9-]+)',
            r'SKADA NR:\s*([0-9-]+)',
            r'DAMAGE NUMBER:\s*([0-9-]+)'
        ]
        
        for pattern in patterns:
            match = reg.search(pattern, info, reg.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_invoice_number(self, info: str) -> Optional[str]:
        """Extract invoice number from info"""
        if pd.isna(info):
            return None
        
        patterns = [
            r'FAKTURANUMMER:\s*([0-9A-Za-z-]+)',
            r'INVOICE NUMBER:\s*([0-9A-Za-z-]+)',
            r'FAKTURA NR:\s*([0-9A-Za-z-]+)'
        ]
        
        for pattern in patterns:
            match = reg.search(pattern, info, reg.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_clinic_name(self, info: str) -> Optional[str]:
        """Extract clinic name from info"""
        if pd.isna(info):
            return None
        
        patterns = [
            r'KLINIK:\s*([^\n\r]+)',
            r'CLINIC:\s*([^\n\r]+)',
            r'VET CLINIC:\s*([^\n\r]+)'
        ]
        
        for pattern in patterns:
            match = reg.search(pattern, info, reg.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_owner_name(self, info: str) -> Optional[str]:
        """Extract owner name from info"""
        if pd.isna(info):
            return None
        
        patterns = [
            r'ÄGARE:\s*([^\n\r]+)',
            r'OWNER:\s*([^\n\r]+)',
            r'DJURÄGARE:\s*([^\n\r]+)'
        ]
        
        for pattern in patterns:
            match = reg.search(pattern, info, reg.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_animal_info(self, info: str) -> Optional[str]:
        """Extract animal information from info"""
        if pd.isna(info):
            return None
        
        patterns = [
            r'DJUR:\s*([^\n\r]+)',
            r'ANIMAL:\s*([^\n\r]+)',
            r'PET:\s*([^\n\r]+)'
        ]
        
        for pattern in patterns:
            match = reg.search(pattern, info, reg.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def validate_payments(self, payments: List[Dict[str, Any]]) -> Tuple[List[PaymentIn], List[str]]:
        """
        Validate payment data
        
        Args:
            payments: Payment data list
            
        Returns:
            Tuple[List[PaymentIn], List[str]]: Valid data and error messages
        """
        valid_payments = []
        errors = []
        
        for i, payment_data in enumerate(payments):
            try:
                payment = PaymentIn(**payment_data)
                valid_payments.append(payment)
            except Exception as e:
                errors.append(f"Payment #{i+1}: {str(e)}")
        
        return valid_payments, errors
    
    def calculate_statistics(self, df: Optional[pd.DataFrame] = None) -> PaymentStatistics:
        """
        Calculate payment statistics
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            PaymentStatistics: Statistics information
        """
        if df is None:
            df = self.processed_payments_df if not self.processed_payments_df.empty else self.payments
        
        if df.empty:
            return PaymentStatistics(
                totalPayments=0,
                matchedPayments=0,
                unmatchedPayments=0,
                matchingRate=0.0,
                totalAmount=0.0,
                matchedAmount=0.0,
                averageAmount=0.0
            )
        
        total_payments = len(df)
        total_amount = df['amount'].sum() if 'amount' in df.columns else 0
        average_amount = df['amount'].mean() if 'amount' in df.columns else 0
        
        # Matching statistics
        matched_payments = 0
        matched_amount = 0
        perfect_matches = 0
        partial_matches = 0
        entity_matches = 0
        payout_matches = 0
        no_matches = 0
        
        if 'status' in df.columns:
            status_counts = df['status'].value_counts()
            matched_payments = len(df[~df['status'].str.contains('No Found|No matching', na=False)])
            
            # Detailed categorization statistics
            perfect_matches = len(df[df['status'].str.contains('One DR matched perfectly', na=False)])
            partial_matches = len(df[df['status'].str.contains('total amount matches', na=False)])
            entity_matches = len(df[df['status'].str.contains('by both entity and amount', na=False)])
            payout_matches = len(df[df['status'].str.contains('paid out', na=False)])
            no_matches = len(df[df['status'].str.contains('No Found|No matching', na=False)])
            
            # Calculate matched amount
            matched_df = df[~df['status'].str.contains('No Found|No matching', na=False)]
            matched_amount = matched_df['amount'].sum() if not matched_df.empty and 'amount' in matched_df.columns else 0
        
        unmatchedPayments = total_payments - matched_payments
        matchingRate = matched_payments / total_payments if total_payments > 0 else 0
        
        return PaymentStatistics(
            totalPayments=total_payments,
            matchedPayments=matched_payments,
            unmatchedPayments=unmatchedPayments,
            matchingRate=matchingRate,
            totalAmount=float(total_amount),
            matchedAmount=float(matched_amount),
            averageAmount=float(average_amount),
            perfectMatches=perfect_matches,
            partialMatches=partial_matches,
            entityMatches=entity_matches,
            payoutMatches=payout_matches,
            noMatches=no_matches
        )
    
    def export_results(self, df: pd.DataFrame, file_path: str, file_format: str = 'json'):
        """
        Export processing results
        
        Args:
            df: Data to export
            file_path: Export path
            file_format: Export format
        """
        try:
            if file_format.lower() == 'json':
                df.to_json(file_path, orient='records', indent=2, force_ascii=False)
            elif file_format.lower() == 'csv':
                df.to_csv(file_path, index=False, encoding='utf-8')
            elif file_format.lower() in ['excel', 'xlsx']:
                df.to_excel(file_path, index=False)
            else:
                raise ValueError(f"Unsupported export format: {file_format}")
                
            print(f"Data exported to: {file_path}")
            
        except Exception as e:
            print(f"Failed to export data: {str(e)}")
    
    def get_payment_by_id(self, payment_id: int) -> Optional[Dict[str, Any]]:
        """
        Get payment record by ID
        
        Args:
            payment_id: Payment ID
            
        Returns:
            Optional[Dict[str, Any]]: Payment record
        """
        df = self.processed_payments_df if not self.processed_payments_df.empty else self.payments
        
        if df.empty:
            return None
        
        payment_records = df[df['id'] == payment_id]
        if payment_records.empty:
            return None
        
        return payment_records.iloc[0].to_dict()
    
    def get_payments_by_status(self, status_pattern: str) -> pd.DataFrame:
        """
        Get payment records by status pattern
        
        Args:
            status_pattern: Status matching pattern
            
        Returns:
            pd.DataFrame: Matching payment records
        """
        df = self.processed_payments_df if not self.processed_payments_df.empty else self.payments
        
        if df.empty or 'status' not in df.columns:
            return pd.DataFrame()
        
        return df[df['status'].str.contains(status_pattern, case=False, na=False)]
    
    def get_unmatched_payments(self) -> pd.DataFrame:
        """
        Get unmatched payment records
        
        Returns:
            pd.DataFrame: Unmatched payment records
        """
        return self.get_payments_by_status('No Found|No matching')
    
    def get_matched_payments(self) -> pd.DataFrame:
        """
        Get matched payment records
        
        Returns:
            pd.DataFrame: Matched payment records
        """
        df = self.processed_payments_df if not self.processed_payments_df.empty else self.payments
        
        if df.empty or 'status' not in df.columns:
            return pd.DataFrame()
        
        return df[~df['status'].str.contains('No Found|No matching', case=False, na=False)]


# Create global instance
payment_dataset = PaymentDataset()
