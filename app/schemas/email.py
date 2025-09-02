from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import json
import regex as reg

class AttachmentIn(BaseModel):
    Name: str
    Content: str
    ContentID: Optional[str] = ""
    ContentType: Optional[str] = None
    ContentLength: Optional[int] = None

class EmailIn(BaseModel):
    id: int
    createdAt: datetime
    from_: str = Field(..., alias="from")  
    to: str
    subject: Optional[str] = None
    textPlain: Optional[str] = None
    textHtml: Optional[str] = None
    attachments: Optional[List[AttachmentIn]] = None 

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
    
    @field_validator("attachments", mode="before")
    @classmethod
    def parse_attachments(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return None
        return v

# class PreProcessedEmailOut(EmailIn):
#     date: datetime 
#     originSender: Optional[str] = None
#     sender: Optional[str] = None
#     source: Optional[str] = None
#     originReceiver: Optional[str] = None
#     receiver: Optional[str] = None
#     sendTo: Optional[str] = None
#     clinicCompType: Optional[str] = None
#     reference: Optional[str] = None
#     insuranceCaseRef: Optional[str] = None
#     errandId: List[int] = Field(default_factory=list)
#     category: Optional[str] = None
#     origin: Optional[str] = None
#     email: Optional[str] = None
    

    
class EmailOut(BaseModel):
    id: int
    from_: str = Field(..., alias="from")   
    sender: Optional[str] = None
    source: Optional[str] = None
    to: Optional[str] = None
    receiver: Optional[str] = None
    insuranceCompanyReference: Optional[str] = None
    category: Optional[str] = None
    errandId: List[int] = Field(default_factory=list)
    totalAmount: Optional[float] = None
    settlementAmount: Optional[float] = None
    reference: Optional[str] = None
    insuranceCaseRef: Optional[str] = None
    insuranceNumber: Optional[str] = None
    damageNumber: Optional[str] = None
    animalName: Optional[str] = None
    ownerName: Optional[str] = None
    note: Optional[str] = None
    showPage: Optional[str] = None
    isStaffAnimal: bool = False

    class Config:
        populate_by_name = True
    
