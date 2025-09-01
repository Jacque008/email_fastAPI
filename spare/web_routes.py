"""
Web Routes - Provides Web interface endpoints
"""
from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import os
from groq import Groq
from typing import Optional

from .email_service import EmailService, EmailSummaryRequest, EmailForwardingIn
from .errand_service import ErrandService, ErrandLogRequest
from .payment_service import PaymentService, PaymentMatchRequest

router = APIRouter(tags=["web"])
templates = Jinja2Templates(directory="templates")

# Initialize services
def get_groq_client():
    api_key = os.getenv('GROQ_API_KEY')
    if api_key:
        return Groq(api_key=api_key)
    return None

email_service = EmailService()
errand_service = ErrandService(groq_client=get_groq_client())
payment_service = PaymentService()


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/summary", response_class=HTMLResponse)
async def summary_page(request: Request):
    """Summary page"""
    return templates.TemplateResponse("summary.html", {
        "request": request,
        "title": "AI Summary Generation"
    })


@router.post("/summary", response_class=HTMLResponse)
async def generate_summary_web(
    request: Request,
    emailId: Optional[int] = Form(None),
    errandNumber: Optional[str] = Form(None),
    reference: Optional[str] = Form(None)
):
    """Handle summary generation request"""
    try:
        # Create request object
        summary_request = EmailSummaryRequest(
            emailId=emailId,
            errandNumber=errandNumber,
            reference=reference
        )
        
        # Generate summary
        result = email_service.generate_summary(summary_request, use_case='webService')
        
        return templates.TemplateResponse("summary.html", {
            "request": request,
            "title": "AI Summary Generation",
            "result": result,
            "emailId": emailId,
            "errandNumber": errandNumber,
            "reference": reference
        })
        
    except Exception as e:
        return templates.TemplateResponse("summary.html", {
            "request": request,
            "title": "AI Summary Generation",
            "error": f"Failed to generate summary: {str(e)}",
            "emailId": emailId,
            "errandNumber": errandNumber,
            "reference": reference
        })


@router.get("/forward", response_class=HTMLResponse)
async def forward_page(request: Request):
    """Forward page"""
    return templates.TemplateResponse("forward.html", {
        "request": request,
        "title": "Email Forwarding Generation"
    })


@router.post("/forward", response_class=HTMLResponse)
async def generate_forward_web(
    request: Request,
    id: int = Form(...),
    recipient: str = Form(...),
    correctedCategory: str = Form(...),
    userId: Optional[int] = Form(None)
):
    """Handle forwarding generation request"""
    try:
        # Create request object
        forward_request = EmailForwardingIn(
            id=id,
            recipient=recipient,
            correctedCategory=correctedCategory,
            userId=userId
        )
        
        # Generate forwarding
        result = email_service.generate_forwarding(forward_request)
        
        return templates.TemplateResponse("forward.html", {
            "request": request,
            "title": "Email Forwarding Generation",
            "result": result,
            "id": id,
            "recipient": recipient,
            "correctedCategory": correctedCategory,
            "userId": userId
        })
        
    except Exception as e:
        return templates.TemplateResponse("forward.html", {
            "request": request,
            "title": "Email Forwarding Generation",
            "error": f"Failed to generate forwarding: {str(e)}",
            "id": id,
            "recipient": recipient,
            "correctedCategory": correctedCategory,
            "userId": userId
        })


@router.get("/log", response_class=HTMLResponse)
async def log_page(request: Request):
    """Log page"""
    return templates.TemplateResponse("log.html", {
        "request": request,
        "title": "Errand Timeline Log"
    })


@router.post("/log", response_class=HTMLResponse)
async def generate_log_web(
    request: Request,
    errandNumber: str = Form(...)
):
    """Handle log generation request"""
    try:
        # Create request object
        log_request = ErrandLogRequest(errandNumber=errandNumber)
        
        # Generate log
        result = errand_service.generate_errand_log(log_request)
        
        if "error" in result:
            return templates.TemplateResponse("log.html", {
                "request": request,
                "title": "Errand Timeline Log",
                "error": result["error"],
                "errandNumber": errandNumber
            })
        
        return templates.TemplateResponse("log.html", {
            "request": request,
            "title": "Errand Timeline Log",
            "group_log": result.get("group_log", {}),
            "group_ai": result.get("group_ai", {}),
            "errandNumber": errandNumber
        })
        
    except Exception as e:
        return templates.TemplateResponse("log.html", {
            "request": request,
            "title": "Errand Timeline Log",
            "error": f"Failed to generate log: {str(e)}",
            "errandNumber": errandNumber
        })


@router.get("/payment", response_class=HTMLResponse)
async def payment_page(request: Request):
    """Payment page"""
    return templates.TemplateResponse("payment.html", {
        "request": request,
        "title": "Payment Matching"
    })


@router.post("/payment", response_class=HTMLResponse)
async def match_payment_web(
    request: Request,
    id: int = Form(...),
    amount: float = Form(...),
    reference: str = Form(...),
    info: str = Form(...),
    bankName: str = Form(...),
    createdAt: str = Form(...)
):
    """Handle payment matching request"""
    try:
        # Create request object
        payment_request = PaymentMatchRequest(
            id=id,
            amount=amount,
            reference=reference,
            info=info,
            bankName=bankName,
            createdAt=createdAt
        )
        
        # Execute matching
        results = payment_service.match_payments([payment_request])
        
        return templates.TemplateResponse("payment.html", {
            "request": request,
            "title": "Payment Matching",
            "results": results,
            "id": id,
            "amount": amount,
            "reference": reference,
            "info": info,
            "bankName": bankName,
            "createdAt": createdAt
        })
        
    except Exception as e:
        return templates.TemplateResponse("payment.html", {
            "request": request,
            "title": "Payment Matching",
            "error": f"Payment matching failed: {str(e)}",
            "id": id,
            "amount": amount,
            "reference": reference,
            "info": info,
            "bankName": bankName,
            "createdAt": createdAt
        })


# Test page
@router.get("/test", response_class=HTMLResponse)
async def test_page(request: Request):
    """Test page"""
    return templates.TemplateResponse("test.html", {
        "request": request,
        "title": "Function Testing"
    })
