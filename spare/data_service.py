"""
Data service - Handle all database access
"""
from typing import Dict, List, Any, Optional
import pandas as pd
from ..app.services.base_service import BaseService
from ..app.services.utils import fetchFromDB

class DataService(BaseService):
    """Data service"""
    
    def __init__(self):
        super().__init__()
        self._setup_queries()
    
    def _setup_queries(self):
        """Setup query statements"""
        try:
            self.forward_summary_info_query = self.queries['forwardSummaryInfo'].iloc[0]
            self.admin_query = self.queries['admin'].iloc[0]
            self.log_base_query = self.queries['logBase'].iloc[0]
            self.summary_chat_query = self.queries['summaryChat'].iloc[0]
            self.summary_email_query = self.queries['summaryEmail'].iloc[0]
            self.summary_comment_query = self.queries['summaryComment'].iloc[0]
        except Exception:
            # Set default empty queries
            self.forward_summary_info_query = ""
            self.admin_query = ""
            self.log_base_query = ""
            self.summary_chat_query = ""
            self.summary_email_query = ""
            self.summary_comment_query = ""
    
    def get_forward_email_data(self, email_id: int) -> Optional[Dict[str, Any]]:
        """Get forwarding email required data"""
        try:
            result = fetchFromDB(self.forward_summary_info_query.format(ID=email_id))
            return result.iloc[0].to_dict() if not result.empty else None
        except:
            return None
    
    def get_admin_name(self, user_id: int) -> str:
        """Get admin name"""
        try:
            result = fetchFromDB(self.admin_query.format(COND=f"id = {user_id}"))
            return result.iloc[0]['firstName'] if not result.empty else ""
        except:
            return ""
    
    def get_errand_base_data(self, errand_number: str) -> Optional[Dict[str, Any]]:
        """Get errand base data"""
        try:
            condition = f"e.errandNumber = '{errand_number}'"
            result = fetchFromDB(self.log_base_query.format(COND1=True, COND2=condition))
            return result.iloc[0].to_dict() if not result.empty else None
        except:
            return None
    
    def get_chat_data(self, condition: str) -> List[Dict[str, Any]]:
        """Get chat data"""
        try:
            result = fetchFromDB(self.summary_chat_query.format(CONDITION=condition))
            return result.to_dict('records') if not result.empty else []
        except:
            return []
    
    def get_email_data(self, condition: str) -> List[Dict[str, Any]]:
        """Get email data"""
        try:
            result = fetchFromDB(self.summary_email_query.format(CONDITION=condition))
            return result.to_dict('records') if not result.empty else []
        except:
            return []
    
    def get_comment_data(self, condition: str) -> List[Dict[str, Any]]:
        """Get comment data"""
        try:
            result = fetchFromDB(self.summary_comment_query.format(CONDITION=condition))
            return result.to_dict('records') if not result.empty else []
        except:
            return []
    
    # Log related data fetching methods - simplified implementation
    def get_errand_create_events(self, base_data: Dict) -> List[Dict]:
        """Get errand creation events"""
        return [{
            'timestamp': base_data.get('errandCreaTime'),
            'itemId': f"errandNr: {base_data.get('errandNumber', '')}",
            'message': '',
            'involved': base_data.get('clinicName', ''),
            'source': '',
            'errandId': base_data.get('errandId', 0)
        }] if base_data.get('errandCreaTime') else []
    
    def get_send_to_ic_events(self, base_data: Dict) -> List[Dict]:
        """Get send to insurance company events"""
        return [{
            'timestamp': base_data.get('sendTime'),
            'itemId': f"insuranceCaseId: {base_data.get('insuranceCaseId', '')}",
            'message': f"reference: {base_data.get('reference', '')}",
            'involved': base_data.get('insuranceCompanyName', ''),
            'source': '',
            'errandId': base_data.get('errandId', 0)
        }] if base_data.get('sendTime') else []
    
    def get_email_events(self, errand_number: str) -> List[Dict]:
        """Get email events"""
        # TODO: Implement specific email event query logic
        return []
    
    def get_chat_events(self, errand_number: str) -> List[Dict]:
        """Get chat events"""
        # TODO: Implement specific chat event query logic
        return []
    
    def get_comment_events(self, errand_number: str) -> List[Dict]:
        """Get comment events"""
        # TODO: Implement specific comment event query logic
        return []
    
    def get_update_events(self, base_data: Dict) -> List[Dict]:
        """Get update events"""
        # TODO: Implement specific update event query logic
        return []
    
    def get_invoice_events(self, errand_number: str) -> List[Dict]:
        """Get invoice events"""
        # TODO: Implement specific invoice event query logic
        return []
    
    def get_payment_events(self, errand_number: str) -> List[Dict]:
        """Get payment events"""
        # TODO: Implement specific payment event query logic
        return []
    
    def get_cancel_events(self, errand_number: str) -> List[Dict]:
        """Get cancellation events"""
        # TODO: Implement specific cancellation event query logic
        return []
