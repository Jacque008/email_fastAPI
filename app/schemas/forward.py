from typing import Optional, Dict, Any
from pydantic import BaseModel

class ForwardingIn(BaseModel):
    """Request data for email forwarding"""
    id: int
    userId: Optional[int] = None

class ForwardingOut(BaseModel):
    """Result data for email forwarding"""
    id: int
    action: str = ""
    forward_address: str = ""
    forward_subject: str = ""
    forward_text: Optional[str] = None
    journal_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        result = {
            "id": self.id,
            "action": self.action,
            "forwardAddress": self.forward_address,
            "forwardSubject": self.forward_subject,
            "forwardText": self.forward_text,
            "journalData": self.journal_data
        }
        if self.error:
            result["error"] = self.error
        return result
