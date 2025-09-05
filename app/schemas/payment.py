from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import regex as reg

class PaymentIn(BaseModel):
    """Input schema for payment matching"""
    id: int
    amount: int  # in cents/öre 
    reference: Optional[str] = None
    info: Optional[str] = None
    bankName: str
    createdAt: datetime

    class Config:
        populate_by_name = True

    @field_validator("createdAt", mode="before")
    def parse_created_at(cls, v):
        if v is None:
            return None

        if isinstance(v, int) or (isinstance(v, str) and reg.fullmatch(r"\d+", v)):
            return datetime.fromtimestamp(int(v) / 1000)

        if isinstance(v, str):
            try:
                return datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f %z")
            except ValueError:
                try:
                    return datetime.strptime(v, "%Y-%m-%d %H:%M:%S %z")
                except ValueError:
                    raise ValueError(f"Unsupported createdAt format: {v}")

        return v

class PaymentOut(PaymentIn):
    """Output schema for payment matching results - inherits from PaymentIn"""
    # Override amount to be formatted string instead of int
    amount: str  # formatted as "X.XX kr"
    
    # Matched info
    insuranceCaseId: List[int] = Field(default_factory=list)
    status: str = ""
    
    # Internal matching data (optional for API response)
    valPay: List[str] = Field(default_factory=list)
    valErrand: List[str] = Field(default_factory=list)
    isReference: List[str] = Field(default_factory=list)
    referenceLink: List[str] = Field(default_factory=list)
    settlementAmount: float = 0.0
    extractReference: Optional[str] = None
    extractDamageNumber: Optional[str] = None
    extractOtherNumber: Optional[str] = None