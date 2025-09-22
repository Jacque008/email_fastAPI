from typing import Optional, Union
from pydantic import BaseModel, Field, model_validator

class SummaryIn(BaseModel):
    """Input schema for summary requests"""
    emailId: Optional[int] = None
    errandNumber: Optional[str] = None
    reference: Optional[str] = None
    
    @model_validator(mode='after')
    def validate_at_least_one(self):
        """Ensure at least one identifier is provided"""
        if not any([self.emailId, self.errandNumber, self.reference]):
            raise ValueError('At least one of emailId, errandNumber, or reference must be provided')
        return self

class SummaryOutWeb(BaseModel):
    """Output schema for web service summary results - includes all detailed summaries"""
    # Individual summaries (for webService use case)
    summary_chat_with_clinic: Optional[str] = Field(default=None, description="Chat summary between DRP and clinic")
    clinic_name: Optional[str] = Field(default=None, description="Name of the clinic")

    summary_chat_with_ic: Optional[str] = Field(default=None, description="Chat summary between DRP and insurance company")
    ic_name: Optional[str] = Field(default=None, description="Name of the insurance company")
    error_chat: Optional[str] = Field(default=None, description="Chat processing error message")

    summary_email_conversation: Optional[str] = Field(default=None, description="Email conversation summary")
    email_sender: Optional[str] = Field(default=None, description="Email sender")
    email_receiver: Optional[str] = Field(default=None, description="Email receiver")
    error_email: Optional[str] = Field(default=None, description="Email processing error message")

    summary_comment_dr: Optional[str] = Field(default=None, description="DR comments summary")
    error_comment_dr: Optional[str] = Field(default=None, description="DR comments error message")

    summary_comment_email: Optional[str] = Field(default=None, description="Email comments summary")
    error_comment_email: Optional[str] = Field(default=None, description="Email comments error message")

    # Combined summary
    summary_combined: Optional[str] = Field(default=None, description="Combined summary of all communications")
    error_combined: Optional[str] = Field(default=None, description="Combined processing error message")

    # General error
    error_message: Optional[str] = Field(default=None, description="General error message")

class SummaryOutAPI(BaseModel):
    """Output schema for API summary results - only combined summary and error"""
    Summary_Combined_Info: Optional[str] = Field(default=None, description="Combined summary of all communications")
    Error_Combined_Info: Optional[str] = Field(default=None, description="Combined processing error message")

# Keep the original SummaryOut for backward compatibility
# SummaryOut = SummaryOutWeb
