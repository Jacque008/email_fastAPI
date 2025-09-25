from fastapi import APIRouter, HTTPException, Depends, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Dict, Any, Optional, Union
import logging
import os
import pandas as pd
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

@router.post("/log")
async def process_log_form(
    request: Request,
    errand_number: str = Form(...),
    _current_user: dict = Depends(get_current_user)
):
    """Process log form submission and return HTML with results"""
    try:
        # Use pure DataFrame approach - API handles Pydantic conversions
        log_df = pd.DataFrame([{'errand_number': errand_number}])
        ds = LogDataset(df=log_df)
        result_df = ds.do_chronological_log()

        if result_df.empty:
            return templates.TemplateResponse("log.html", {
                "request": request,
                "title": "Kronologiska Loggar",
                "errand_number": errand_number,
                "error": f"No log data found for errand {errand_number}"
            })

        # Convert DataFrame result to Pydantic model here in API layer
        result_data = result_df.iloc[0].to_dict()
        clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
        log_data = LogOut(**clean_data)

        if log_data.error_message:
            return templates.TemplateResponse("log.html", {
                "request": request,
                "title": "Kronologiska Loggar",
                "errand_number": errand_number,
                "error": log_data.error_message
            })

        # Format data for template
        group_log = {
            log_data.errand_id: {
                "title": log_data.log_title,
                "content": log_data.log_content
            }
        }
        group_ai = {
            log_data.errand_id: log_data.ai_analysis
        }

        return templates.TemplateResponse("log.html", {
            "request": request,
            "title": "Kronologiska Loggar",
            "errand_number": errand_number,
            "group_log": group_log,
            "group_ai": group_ai
        })

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


@router.get("/log/{errand_number}", response_model=LogOut)
async def get_log_by_errand_number(errand_number: str):
    """
    Get chronological log for a specific errand number (GET endpoint)

    This is a convenience endpoint that accepts errand number as a path parameter
    instead of request body.
    """
    try:
        # Use pure DataFrame approach - API handles Pydantic conversions
        log_df = pd.DataFrame([{'errand_number': errand_number}])
        ds = LogDataset(df=log_df)
        result_df = ds.do_chronological_log()

        if result_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No log data found for errand {errand_number}"
            )

        # Convert DataFrame result to Pydantic model here in API layer
        result_data = result_df.iloc[0].to_dict()
        clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
        return LogOut(**clean_data)

    except Exception as e:
        logger.error(f"Error getting log for errand {errand_number}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get log for errand {errand_number}: {str(e)}"
        )


@router.post("/log_batch_api", response_model=List[LogOut])
async def generate_batch_logs_api(log_data: List[Dict[str, Any]]):
    """
    Generate chronological logs for multiple errands efficiently.

    Expected format: [{"errandNumber": "12345"}, {"errandNumber": "67890"}]
    """
    try:
        if not log_data:
            raise HTTPException(status_code=400, detail="Log data cannot be empty")

        log_requests = []
        for i, data in enumerate(log_data):
            errand_number = data.get("errandNumber")
            if not errand_number:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing 'errandNumber' in request item {i}"
                )
            log_requests.append(LogIn(errand_number=errand_number))

        # Use pure DataFrame approach for batch processing
        log_df = pd.DataFrame([{
            'errand_number': lr.errand_number
        } for lr in log_requests])
        ds = LogDataset()
        result_df = ds.do_batch_logs(log_df)

        if result_df.empty:
            return []

        # Convert DataFrame results to Pydantic models here in API layer
        results = []
        for _, row in result_df.iterrows():
            result_data = row.to_dict()
            clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
            results.append(LogOut(**clean_data))

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating batch logs: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate batch logs: {str(e)}"
        )

@router.post("/log_api", response_model=Union[LogOut, List[LogOut]])
async def generate_log_api(log_data: Union[Dict[str, Any], List[Dict[str, Any]]]):
    """
    Generate chronological log(s) for errand(s) - accepts JSON format

    Expected format:
    Single log: {"errandNumber": "12345"}
    Multiple logs: [{"errandNumber": "12345"}, {"errandNumber": "67890"}]

    This endpoint creates detailed chronological logs showing all activities
    related to errands, including:
    - Errand creation and submission
    - Email correspondence
    - Chat communications
    - Comments
    - Invoice generation
    - Payment transactions
    - Cancellations and reversals
    """
    try:
        # Handle single request
        if isinstance(log_data, dict):
            errand_number = log_data.get("errandNumber")
            if not errand_number:
                raise HTTPException(
                    status_code=400,
                    detail="Missing 'errandNumber' in request data"
                )

            log_request = LogIn(errand_number=errand_number)

            # Use pure DataFrame approach - API handles Pydantic conversions
            log_df = pd.DataFrame([{'errand_number': log_request.errand_number}])
            ds = LogDataset(df=log_df)
            result_df = ds.do_chronological_log()

            if result_df.empty:
                raise HTTPException(
                    status_code=404,
                    detail=f"No log data found for errand {errand_number}"
                )

            # Convert DataFrame result to Pydantic model here in API layer
            result_data = result_df.iloc[0].to_dict()
            clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
            return LogOut(**clean_data)

        # Handle batch requests
        elif isinstance(log_data, list):
            if not log_data:
                raise HTTPException(status_code=400, detail="Log data cannot be empty")

            log_requests = []
            for i, data in enumerate(log_data):
                errand_number = data.get("errandNumber")
                if not errand_number:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing 'errandNumber' in request item {i}"
                    )
                log_requests.append(LogIn(errand_number=errand_number))

            # Use pure DataFrame approach for batch processing
            log_df = pd.DataFrame([{
                'errand_number': lr.errand_number
            } for lr in log_requests])
            ds = LogDataset()
            result_df = ds.do_batch_logs(log_df)

            if result_df.empty:
                return []

            # Convert DataFrame results to Pydantic models here in API layer
            results = []
            for _, row in result_df.iterrows():
                result_data = row.to_dict()
                clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
                results.append(LogOut(**clean_data))

            return results

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid request format. Expected JSON object or array with log data"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating log: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate chronological log: {str(e)}"
        )

@router.post("/log_stats")
async def log_statistics(
    log_data: Union[Dict[str, Any], List[Dict[str, Any]]]
):
    """
    Get statistics about generated logs - accepts JSON format

    Args:
        log_data: Dict or List[Dict] with errandNumber

    Returns:
        Statistics about log generation (single dict or list of dicts)
    """
    try:
        # Handle single request
        if isinstance(log_data, dict):
            errand_number = log_data.get("errandNumber")
            if not errand_number:
                raise HTTPException(
                    status_code=400,
                    detail="Missing 'errandNumber' in request data"
                )

            log_request = LogIn(errand_number=errand_number)

            # Use pure DataFrame approach - API handles all conversions
            log_df = pd.DataFrame([{'errand_number': log_request.errand_number}])
            ds = LogDataset(df=log_df)
            stats_df = ds.get_statistics()

            if stats_df.empty:
                return {'has_error': True, 'error_message': 'No statistics available'}

            stats_dict = stats_df.iloc[0].to_dict()
            # Remove NaN values
            stats = {k: v for k, v in stats_dict.items() if pd.notna(v)}
            return stats

        # Handle batch requests
        elif isinstance(log_data, list):
            if not log_data:
                raise HTTPException(status_code=400, detail="Log data cannot be empty")

            log_requests = []
            for i, data in enumerate(log_data):
                errand_number = data.get("errandNumber")
                if not errand_number:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing 'errandNumber' in request item {i}"
                    )
                log_requests.append(LogIn(errand_number=errand_number))

            # Use pure DataFrame approach for batch statistics
            log_df = pd.DataFrame([{
                'errand_number': lr.errand_number
            } for lr in log_requests])
            ds = LogDataset(df=log_df)
            stats_df = ds.get_statistics()

            if stats_df.empty:
                return [{'has_error': True, 'error_message': 'No statistics available'}] * len(log_requests)

            results = []
            for _, row in stats_df.iterrows():
                stats_dict = row.to_dict()
                # Remove NaN values
                stats = {k: v for k, v in stats_dict.items() if pd.notna(v)}
                results.append(stats)

            return results

        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid request format. Expected JSON object or array with log data"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating log statistics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Statistics generation failed: {str(e)}"
        )

