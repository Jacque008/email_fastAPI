from typing import Optional
from pydantic import BaseModel, Field

class LogIn(BaseModel):
    """Input schema for chronological log requests"""
    errandNumber: str = Field(description="Errand number to generate log for")

class LogOut(BaseModel):
    """Output schema for chronological log results"""
    Title: str = Field(description="Log title with payment discrepancy info")
    Chronological_Log: str = Field(description="Formatted chronological log content")
    AI_Analysis: str = Field(description="AI risk assessment and analysis")
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
