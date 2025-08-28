"""
API Routes - Provides API endpoints for email, errand and payment functionality
"""
from typing import List
from fastapi import APIRouter, HTTPException, Depends
import os
from groq import Groq

from .email_service import (
    EmailService, 
    EmailSummaryRequest, 
    EmailSummaryResponse, 
    EmailForwardingIn, 
    EmailForwardingResponse
)
from .errand_service import (
    ErrandService, 
    ErrandLogRequest, 
    ErrandLogResponse
)
from .payment_service import (
    PaymentService, 
    PaymentMatchRequest, 
    PaymentMatchResponse
)

router = APIRouter(prefix="/api", tags=["api"])

# Initialize services
def get_groq_client():
    api_key = os.getenv('GROQ_API_KEY')
    if api_key:
        return Groq(api_key=api_key)
    return None

email_service = EmailService()
errand_service = ErrandService(groq_client=get_groq_client())
payment_service = PaymentService()


@router.post("/summary", response_model=dict)
async def generate_summary(request: EmailSummaryRequest):
    """
    AI Summary API
    
    Generate AI summary based on emailId, errandNumber or reference
    """
    try:
        result = email_service.generate_summary(request, use_case='api')
        
        return {
            "Error_Message": result.get('error_combine'),
            "Summary_Chat_with_Clinic": result.get('summary_clinic'),
            "Summary_Chat_with_IC": result.get('summary_ic'), 
            "Summary_Email_Conversation": result.get('summary_combine')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate summary: {str(e)}")


@router.post("/forward", response_model=EmailForwardingResponse)
async def generate_forwarding(request: EmailForwardingIn):
    """
    Forwarding API
    
    Generate forwarding content for specified email
    """
    try:
        result = email_service.generate_forwarding(request)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate forwarding: {str(e)}")


@router.post("/log", response_model=dict)
async def generate_log(request: ErrandLogRequest):
    """
    Timeline Log API
    
    Generate timeline log and AI risk assessment for errands
    """
    try:
        result = errand_service.generate_errand_log(request)
        
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        
        # 格式化返回结果 - 简化版
        group_log = result.get("group_log", {})
        group_ai = result.get("group_ai", {})
        
        if group_log:
            errand_id = list(group_log.keys())[0]
            log_content = group_log[errand_id].get("content", "")
            ai_analysis = group_ai.get(errand_id, "")
            
            return {
                "errand_id": errand_id,
                "log_content": log_content,
                "ai_analysis": ai_analysis
            }
        else:
            raise HTTPException(status_code=404, detail="Errand data not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate log: {str(e)}")


@router.post("/payment", response_model=List[PaymentMatchResponse])
async def match_payments(requests: List[PaymentMatchRequest]):
    """
    Payment Matching API
    
    Intelligent payment matching
    """
    try:
        results = payment_service.match_payments(requests)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Payment matching failed: {str(e)}")


# Health check endpoint
@router.get("/health")
async def health_check():
    """Health check"""
    return {"status": "healthy", "message": "API service is running normally"}
