"""
Combined text content related data models
"""
from typing import Optional
from pydantic import BaseModel

class CombinedTextRequest(BaseModel):
    """Combined text request"""
    errand_id: Optional[int] = None
    email_id: Optional[int] = None
    reference: Optional[str] = None

class CombinedTextResponse(BaseModel):
    """Combined text response"""
    summary: str
    error_message: Optional[str] = None
