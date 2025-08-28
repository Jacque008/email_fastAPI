from typing import Optional
from pydantic import BaseModel
from datetime import datetime

class ErrandIn(BaseModel):
    errandId: int
    errandNumber: str
    date: datetime
    insuranceCompany: str
    clinicName: str
    totalAmount: Optional[float] = None
    settlementAmount: Optional[float] = None
    reference: Optional[str] = None
    insuranceNumber: Optional[str] = None
    damageNumber: Optional[str] = None
    invoiceReference: Optional[str] = None
    animalName: Optional[str] = None
    ownerName: Optional[str] = None
    paymentOption: Optional[str] = None
    strategyType: Optional[str] = None
    settled: Optional[bool] = None