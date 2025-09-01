"""
Forward email business object
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from ..schemas.fw_email import ForwardingIn

@dataclass
class ForwardingEmail:
    """Forward email business object"""
    # API input fields
    email_id: int
    corrected_category: str
    recipient: str
    user_id: Optional[int] = None
    
    # Database retrieved fields
    original_subject: str = ""
    original_content: str = ""
    sender: str = ""
    reference: str = ""
    text_html: str = ""
    source: str = ""
    send_to: str = ""
    
    # Generated fields
    forward_address: str = ""
    forward_subject: str = ""
    forward_text: str = ""
    
    
    @classmethod
    def from_request(cls, request: ForwardingIn, db_data: Dict[str, Any]) -> "ForwardingEmail":
        """Create object from API request and database data"""
        return cls(
            email_id=request.id,
            corrected_category=request.corrected_category,
            recipient=request.recipient,
            user_id=request.user_id,
            original_subject=db_data.get('subject', ''),
            original_content=db_data.get('email', ''),
            sender=db_data.get('sender', ''),
            reference=db_data.get('reference', ''),
            text_html=db_data.get('textHtml', ''),
            source=db_data.get('source', ''),
            send_to=db_data.get('sendTo', '')
        )

