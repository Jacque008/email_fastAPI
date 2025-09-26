from typing import Optional, Dict, Any
from pydantic import BaseModel

class ForwardingIn(BaseModel):
    """Request data for email forwarding"""
    id: int
    userId: Optional[int] = None

class ForwardingOut(BaseModel):
    """Result data for email forwarding"""
    id: int
    action: str = "forwarding"
    forward_address: str = ""
    forward_subject: str = ""
    forward_text: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "id": self.id,
            "action": self.action,
            "forwardAddress": self.forward_address,
            "forwardSubject": self.forward_subject,
            "forwardText": self.forward_text
        }
