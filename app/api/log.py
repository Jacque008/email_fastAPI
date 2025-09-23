from fastapi import APIRouter, HTTPException, Depends, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Dict, Any, Optional
import logging
import os
from ..schemas.log import LogIn, LogOut
from ..dataset.log_dataset import LogDataset
from ..core.auth import get_current_user
from pydantic import BaseModel

# Setup logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Initialize dataset
log_dataset = LogDataset()

# Setup templates
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

class BatchLogRequest(BaseModel):
    """Schema for batch log requests"""
    errand_numbers: List[str]

class LogStatsResponse(BaseModel):
    """Schema for log statistics response"""
    errand_id: int
    has_error: bool
    error_message: Optional[str] = None
    entry_count: int
    has_ai_analysis: bool
    has_high_risk: bool = False
    has_payment_discrepancy: bool = False
    log_length: int

@router.get("/log")
async def log_page(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """Log page - GET with required authentication"""
    return templates.TemplateResponse("log.html", {"request": request})

@router.post("/log", response_class=HTMLResponse)
async def process_log_form(
    request: Request,
    errand_number: str = Form(description="Errand number from form"),
    _current_user: dict = Depends(get_current_user)
):
    """Process log form submission and return HTML with results"""
    try:
        log_input = LogIn(errand_number=errand_number)
        log_data = log_dataset.generate_chronological_log(log_input)
        context = {
            "request": request,
            "title": "Kronologiska Loggar",
            "errand_number": errand_number,
            "error": None,
            "group_log": None,
            "group_ai": None
        }

        if log_data.error_message:
            context["error"] = log_data.error_message
            logger.warning(f"Log generation error: {log_data.error_message}")
        else:
            group_log = {
                log_data.errand_id: {
                    "title": log_data.log_title,
                    "content": log_data.log_content
                }
            }
            group_ai = {
                log_data.errand_id: log_data.ai_analysis
            }
            context.update({
                "group_log": group_log,
                "group_ai": group_ai
            })
            logger.info(f"Successfully generated log for errand {log_data.errand_id}")

        return templates.TemplateResponse("log.html", context)

    except Exception as e:
        logger.error(f"Error processing log form: {str(e)}")
        return templates.TemplateResponse("log.html", {
            "request": request,
            "title": "Kronologiska Loggar",
            "errand_number": errand_number,
            "error": f"Error: {str(e)}",
            "group_log": None,
            "group_ai": None
        })


@router.get("/log/{errand_number}", response_model=LogOut, summary="Get Log by Errand Number")
async def get_log_by_errand_number(errand_number: str):
    """
    Get chronological log for a specific errand number (GET endpoint).
    
    This is a convenience endpoint that accepts errand number as a path parameter
    instead of request body.
    """
    try:
        # Call the main log_api endpoint with proper format
        request_data = [{"Errand Number": errand_number}]
        return await generate_log_api(request_data)
        
    except Exception as e:
        logger.error(f"Error getting log for errand {errand_number}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get log for errand {errand_number}: {str(e)}"
        )


@router.post("/log_api", response_model=LogOut, summary="Generate Chronological Log (JSON)")
async def generate_log_api(request_data: List[Dict[str, Any]]):
    """
    Generate a chronological log for a specific errand (returns JSON).

    This endpoint creates a detailed chronological log showing all activities
    related to an errand, including:
    - Errand creation and submission
    - Email correspondence
    - Chat communications
    - Comments
    - Invoice generation
    - Payment transactions
    - Cancellations and reversals

    Expected format: [{"errandNumber": "12345"}]
    """
    try:
        if not request_data or len(request_data) == 0:
            raise HTTPException(status_code=400, detail="Request data cannot be empty")

        first_request = request_data[0]
        errand_number = first_request.get("errandNumber")

        if not errand_number:
            raise HTTPException(
                status_code=400,
                detail="Missing 'Errand Number' in request data"
            )

        logger.info(f"Generating log for errand: {errand_number}")

        log_input = LogIn(errand_number=errand_number)
        result = log_dataset.generate_chronological_log(log_input)

        if result.error_message:
            logger.warning(f"Log generation warning for {errand_number}: {result.error_message}")
        else:
            logger.info(f"Successfully generated log for errand {result.errand_id}")

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating log for {errand_number if 'errand_number' in locals() else 'unknown'}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate chronological log: {str(e)}"
        )

