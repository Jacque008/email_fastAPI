from typing import Optional
from pydantic import BaseModel, Field

class LogIn(BaseModel):
    """Input schema for chronological log requests"""
    errand_number: str = Field(description="Errand number to generate log for")

class LogOut(BaseModel):
    """Output schema for chronological log results"""
    errand_id: int = Field(description="The errand ID")
    log_title: str = Field(description="Log title with payment discrepancy info")
    log_content: str = Field(description="Formatted chronological log content")
    ai_analysis: str = Field(description="AI risk assessment and analysis")
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
