"""
Payment Schema Models
支付相关的Pydantic模型定义
"""
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class PaymentIn(BaseModel):
    id: int
    amount: float
    reference: Optional[str] = None
    info: Optional[str] = None
    bankName: str
    createdAt: str
    
    @field_validator("amount", mode="before")
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError('The amount must greater than 0')
        return v
    
    @field_validator('createdAt', mode="before")
    def validate_created_at(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('Invalid time format，pls use ISO format')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "id": 44353,
                "amount": 113700.0,
                "reference": "1000522704",
                "info": "SKADEUTBETALNING\n\nSKADENUMMER: 7160833-1\nFAKTURANUMMER: 11240013235\nSPECIFIKATION ENLIGT DIREKTREGLERING\n",
                "bankName": "Agria Djurförsäkring",
                "createdAt": "2024-11-05T13:49:18.137+01:00"
            }
        }


class PaymentOut(BaseModel):
    id: int
    reference: Optional[str] = None
    bankName: str
    amount: str  
    info: Optional[str] = None
    createdAt: str
    insuranceCaseId: List[int] = Field(default_factory=list)
    status: str

    extractReference: Optional[str] = None
    extractDamageNumber: Optional[str] = None
    extractOtherNumber: Optional[str] = None
    settlementAmount: Optional[float] = None
    referenceLink: List[str] = Field(default_factory=list)
    
    class Config:
        schema_extra = {
            "example": {
                "id": 44353,
                "reference": "1000522704",
                "bankName": "Agria Djurförsäkring",
                "amount": "1137.00 kr",
                "info": "SKADEUTBETALNING...",
                "createdAt": "2024-11-05 13:49:18+01:00",
                "insuranceCaseId": [52517],
                "status": "One DR matched perfectly",
                "extractReference": "1000522704",
                "extractDamageNumber": "7160833",
                "extractOtherNumber": None,
                "settlementAmount": 113700.0,
                "referenceLink": ["<a href='...'>1000522704</a>"]
            }
        }


class PaymentBatch(BaseModel):
    payments: List[PaymentIn]
    
    @field_validator('payments', mode="before")
    def validate_payments_not_empty(cls, v):
        if not v:
            raise ValueError('Payment list cannot be empty')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "payments": [
                    {
                        "id": 44353,
                        "amount": 113700.0,
                        "reference": "1000522704",
                        "info": "SKADEUTBETALNING...",
                        "bankName": "Agria Djurförsäkring",
                        "createdAt": "2024-11-05T13:49:18.137+01:00"
                    }
                ]
            }
        }


class PaymentBatchResult(BaseModel):
    total: int
    successful: int
    failed: int
    results: List[PaymentOut]
    errors: List[Dict[str, Any]] = Field(default_factory=list, description="errors list")
    
    class Config:
        schema_extra = {
            "example": {
                "total": 1,
                "successful": 1,
                "failed": 0,
                "results": [
                    {
                        "id": 44353,
                        "status": "One DR matched perfectly",
                        "insuranceCaseId": [52517]
                    }
                ],
                "errors": []
            }
        }


class PaymentMatchConfig(BaseModel):
    enableAmountMatching: bool = Field(True, description="amount matching")
    enableEntityMatching: bool = Field(True, description="entity matching")
    enablePayoutMatching: bool = Field(True, description="payout matching")
    matchingThreshold: float = Field(0.8, description="payment matching threshold")
    maxDaysGap: int = Field(90, description="max days gap for matching")
    
    @field_validator('matchingThreshold', mode="before")
    def validate_threshold(cls, v):
        if not 0 < v <= 1:
            raise ValueError('payment matching threshold must be between 0 and 1')
        return v
    
    @field_validator('maxDaysGap', mode="before")
    def validate_max_days(cls, v):
        if v < 1 or v > 365:
            raise ValueError('max days gap must be between 1 and 365')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "enableAmountMatching": True,
                "enableEntityMatching": True,
                "enablePayoutMatching": True,
                "matchingThreshold": 0.8,
                "maxDaysGap": 90
            }
        }


class PaymentStatistics(BaseModel):
    totalPayments: int
    matchedPayments: int
    unmatchedPayments: int
    matchingRate: float
    totalAmount: float
    matchedAmount: float
    averageAmount: float
    
    perfectMatches: int = Field(0, description="perfect match count")
    partialMatches: int = Field(0, description="partial match count")
    entityMatches: int = Field(0, description="entity match count")
    payoutMatches: int = Field(0, description="payout match count")
    noMatches: int = Field(0, description="no match count")
    
    class Config:
        schema_extra = {
            "example": {
                "totalPayments": 100,
                "matchedPayments": 85,
                "unmatchedPayments": 15,
                "matchingRate": 0.85,
                "totalAmount": 1500000.0,
                "matchedAmount": 1275000.0,
                "averageAmount": 15000.0,
                "perfectMatches": 65,
                "partialMatches": 10,
                "entityMatches": 10,
                "payoutMatches": 5,
                "noMatches": 10
            }
        }


PaymentRequest = PaymentIn
PaymentResponse = PaymentOut
PaymentMatchRequest = PaymentIn
PaymentMatchResponse = PaymentOut
