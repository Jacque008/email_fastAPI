from typing import Optional
from pydantic import BaseModel

class CombinedTextRequest(BaseModel):
    errand_id: Optional[int] = None
    email_id: Optional[int] = None
    reference: Optional[str] = None

class CombinedTextResponse(BaseModel):
    summary: str
    error_message: Optional[str] = None
