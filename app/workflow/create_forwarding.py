from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from ..schemas.fw_email import ForwardingIn, ForwardingOut
from ..services.base_service import BaseService
from ..services.services import DefaultServices
from ..dataset.forwarding_dataset import ForwardingEmailDataset


@dataclass
class EmailForwardingWorkflow(BaseService):
    services: DefaultServices = field(default_factory=DefaultServices)
    
    def do_single_forwarding(self, request: ForwardingIn) -> ForwardingOut:
        try:
            ds = ForwardingEmailDataset(services=self.services)
            ds.initialize_dataframe(request)\
              .enrich_with_email_data()\
              .clean_email_content()\
              .validate_data()
            
            result = ForwardingOut(id=request.email_id)
            
            if ds.df.empty:
                result.error_message = "Failed to retrieve email data"
                return result

            row_data = ds.df.iloc[0].to_dict()
            result = ds.generate_forward_address(result, row_data, request.recipient)
            result = ds.generate_forward_subject(result, row_data, request.corrected_category)
            result = ds.generate_forward_content(result, row_data, request.corrected_category, request.user_id)
            
            return ForwardingEmailDataset.finalize_result(result)
            
        except Exception as e:
            return ForwardingOut(
                id=request.email_id,
                error_message=f"Forwarding processing failed: {str(e)}"
            )
    
    def do_batch_forwarding(self, requests: List[ForwardingIn]) -> List[ForwardingOut]:
        """Process multiple forwarding requests"""
        results = []
        for request in requests:
            result = self.do_single_forwarding(request)
            results.append(result)
        return results

# Factory function for creating workflow instances
def create_forwarding_workflow() -> EmailForwardingWorkflow:
    """Factory function to create EmailForwardingWorkflow instance"""
    return EmailForwardingWorkflow()


# Convenience functions for external usage
def process_single_forwarding(email_id: int, recipient: str, 
                            corrected_category: str, user_id: Optional[int] = None) -> Dict[str, Any]:
    """Process single forwarding request and return dict result"""
    workflow = create_forwarding_workflow()
    request = ForwardingIn(
        email_id=email_id,
        recipient=recipient,
        corrected_category=corrected_category,
        user_id=user_id
    )
    result = workflow.do_single_forwarding(request)
    return result.to_dict()


def process_batch_forwarding(forwarding_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process batch forwarding requests and return dict results"""
    workflow = create_forwarding_workflow()
    requests = [
        ForwardingIn(
            email_id=data['email_id'],
            recipient=data['recipient'],
            corrected_category=data['corrected_category'],
            user_id=data.get('user_id')
        )
        for data in forwarding_data
    ]
    results = workflow.do_batch_forwarding(requests)
    return [result.to_dict() for result in results]


