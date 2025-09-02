from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from ..schemas.forwarding_schema import ForwardingIn, ForwardingOut
from ..services.base_service import BaseService
from ..services.services import DefaultServices
from ..dataset.forwarding_dataset import ForwardingEmailDataset


@dataclass
class EmailForwardingWorkflow(BaseService):
    services: DefaultServices = field(default_factory=DefaultServices)
    
    def do_single_forwarding(self, request: ForwardingIn) -> ForwardingOut:
        try:
            ds = ForwardingEmailDataset(services=self.services)
            ds.init_fw_email(request)\
              .enrich_email_data()\
              .clean_email_content()\
              .validate_data()
            
            result = ForwardingOut(id=request.email_id)

            if ds.df.empty:
                return result

            row_data = ds.df.iloc[0].to_dict()
            result = ds.generate_forward_address(result, row_data)
            result = ds.generate_forward_subject(result, row_data)
            result = ds.generate_forward_content(result, row_data)
            
            return result
            
        except Exception as e:
            raise Exception(f"Forwarding processing failed: {str(e)}")

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
def process_single_forwarding(email_id: int, receiver: str, 
                            corrected_category: str, user_id: Optional[int] = None) -> Dict[str, Any]:
    """Process single forwarding request and return dict result"""
    workflow = create_forwarding_workflow()
    request = ForwardingIn(
        email_id=email_id,
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
            user_id=data.get('user_id')
        )
        for data in forwarding_data
    ]
    results = workflow.do_batch_forwarding(requests)
    return [result.to_dict() for result in results]


