"""
Workflow routes - 5 core functionality APIs based on Dashboard entry points
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict
from ..app.schemas.fw_email import ForwardingemailRequest, ForwardingemailResponse
from .log import ErrandLogRequest, ErrandLogResponse
from .text_content import CombinedTextRequest, CombinedTextResponse
from ..app.schemas.email import EmailIn, EmailOut
from .payment import PaymentMatchRequest, PaymentMatchResponse
from .business_workflow_service import BusinessWorkflowService

router = APIRouter(prefix="/workflows", tags=["workflows"])

def get_workflow_service() -> BusinessWorkflowService:
    return BusinessWorkflowService()

@router.post("/email-categorization", response_model=List[EmailOut])
async def categorize_and_connect_emails(
    emails: List[EmailIn],
    workflow_service: BusinessWorkflowService = Depends(get_workflow_service)
):
    """
    Email categorization and errand connection workflow
    Dashboard function: Email Category&Connect (to errand)
    """
    try:
        emails_data = [email.dict() for email in emails]
        results = workflow_service.process_email_categorization(emails_data)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/email-forwarding", response_model=ForwardingemailResponse)
async def create_email_forwarding(
    request: ForwardingemailRequest,
    workflow_service: BusinessWorkflowService = Depends(get_workflow_service)
):
    """
    Email forwarding workflow
    Dashboard function: Email Forwarding
    """
    try:
        return workflow_service.create_forward_email(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/payment-matching", response_model=List[PaymentMatchResponse])
async def match_payments(
    payments: List[PaymentMatchRequest],
    workflow_service: BusinessWorkflowService = Depends(get_workflow_service)
):
    """
    Payment matching workflow
    Dashboard function: Payment Matching
    """
    try:
        payments_data = [payment.dict() for payment in payments]
        results = workflow_service.process_payment_matching(payments_data)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/log-generation", response_model=ErrandLogResponse)
async def generate_errand_log(
    request: ErrandLogRequest,
    workflow_service: BusinessWorkflowService = Depends(get_workflow_service)
):
    """
    Errand log generation and analysis workflow
    Dashboard function: Log Generating and Analysing
    """
    try:
        return workflow_service.generate_errand_log(request)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/text-summarization", response_model=CombinedTextResponse)
async def summarize_combined_text(
    request: CombinedTextRequest,
    workflow_service: BusinessWorkflowService = Depends(get_workflow_service)
):
    """
    Combined text summarization workflow
    Dashboard function: Combined Text Summarizing
    """
    try:
        return workflow_service.generate_combined_text_summary(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
