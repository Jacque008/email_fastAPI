from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator

class ForwardingIn(BaseModel):
    """Request data for email forwarding"""
    email_id: int
    recipient: str
    corrected_category: str
    user_id: Optional[int] = None


class ForwardingOut(BaseModel):
    """Result data for email forwarding"""
    id: int
    forward_address: str = ""
    forward_subject: str = ""
    forward_text: str = ""
    success: bool = False
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "id": self.id,
            "forwardAddress": self.forward_address,
            "forwardSubject": self.forward_subject,
            "forwardText": self.forward_text,
            "success": self.success,
            "errorMessage": self.error_message
        }
