"""
Log related data models
"""
from typing import Optional
from pydantic import BaseModel

class ErrandLogRequest(BaseModel):
    """Errand log request"""
    errand_number: str

class ErrandLogResponse(BaseModel):
    """Errand log response"""
    errand_id: int
    timeline_html: str
    ai_analysis: str
    risk_score: Optional[float] = None
