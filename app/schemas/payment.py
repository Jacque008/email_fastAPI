from typing import Optional, List, Dict, Any
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
    amount: str  # formatted as "X.XX kr"
    insuranceCaseId: List[int] = Field(default_factory=list)
    status: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format, excluding commented fields"""
        return {
            "id": self.id,
            "bankName": self.bankName,
            "amount": self.amount,
            "info": self.info,
            "reference": self.reference,
            "createdAt": self.createdAt,
            "insuranceCaseId": self.insuranceCaseId,
            "status": self.status
        }