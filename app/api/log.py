from fastapi import APIRouter, HTTPException, Depends, Request, Form, status
from fastapi.templating import Jinja2Templates
from typing import List
import logging
import os
import pandas as pd
from ..schemas.log import LogIn, LogOut
from ..dataset.log_dataset import LogDataset
from ..core.auth import get_current_user

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

        result_data = result_df.iloc[0].to_dict()
        clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}

        if clean_data.get('error_message'):
            return templates.TemplateResponse("log.html", {
                "request": request,
                "title": "Kronologiska Loggar",
                "errand_number": errand_number,
                "error": clean_data['error_message']
            })

        group_log = {
            1: {
                "title": clean_data.get('Title', ''),
                "content": clean_data.get('Chronological_Log', '')
            }
        }
        group_ai = {
            1: clean_data.get('AI_Analysis', '')
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

@router.post("/log_api", response_model=LogOut)
async def generate_log_api(log_data: List[LogIn]):
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
        if not log_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Log data cannot be empty."
            )

        log_records = []
        for log in log_data:
            record = log.model_dump(by_alias=True)
            if 'errandNumber' in record:
                record['errand_number'] = record.pop('errandNumber')
            log_records.append(record)

        log_df = pd.DataFrame(log_records)
        ds = LogDataset(df=log_df)
        result_df = ds.do_chronological_log()

        if result_df.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No log data found for errand {log_data[0].errandNumber}"
            )

        if len(result_df) > 0:
            result_data = result_df.iloc[0].to_dict()
            clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
            title = clean_data.get('Title', 'Unknown Errand')
            nested_result = {
                title: {
                    "AI_Analysis": clean_data.get('AI_Analysis', ''),
                    "Chronological_Log": clean_data.get('Chronological_Log', '')
                }
            }

            return LogOut(root=nested_result)
        else:
            return LogOut(root={})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating log: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate chronological log: {str(e)}"
        )
 
# @router.get("/log/{errand_number}", response_model=LogOut)
# async def get_log_by_errand_number(errand_number: str):
#     """
#     Get chronological log for a specific errand number (GET endpoint)

#     This is a convenience endpoint that accepts errand number as a path parameter
#     instead of request body.
#     """
#     try:
#         # Use pure DataFrame approach - API handles Pydantic conversions
#         log_df = pd.DataFrame([{'errand_number': errand_number}])
#         ds = LogDataset(df=log_df)
#         result_df = ds.do_chronological_log()

#         if result_df.empty:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"No log data found for errand {errand_number}"
#             )

#         # Convert DataFrame result to Pydantic model here in API layer
#         result_data = result_df.iloc[0].to_dict()
#         clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
#         return LogOut(**clean_data)

#     except Exception as e:
#         logger.error(f"Error getting log for errand {errand_number}: {str(e)}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"Failed to get log for errand {errand_number}: {str(e)}"
#         )
 
# @router.post("/log_stats")
# async def log_statistics(log_data: List[LogIn]):
#     """
#     Get statistics about generated logs - accepts JSON format

#     Args:
#         log_data: Dict or List[Dict] with errandNumber

#     Returns:
#         Statistics about log generation (single dict or list of dicts)
#     """
#     try:
#         if not log_data:
#             raise HTTPException(status_code=400, detail="Log data cannot be empty")

#         # Map errandNumber to errand_number for dataset compatibility
#         log_records = []
#         for log in log_data:
#             record = log.model_dump(by_alias=True)
#             if 'errandNumber' in record:
#                 record['errand_number'] = record.pop('errandNumber')
#             log_records.append(record)

#         log_df = pd.DataFrame(log_records)
#         ds = LogDataset(df=log_df)
#         stats_df = ds.get_statistics()

#         if stats_df.empty:
#             return {'has_error': True, 'error_message': 'No statistics available'}

#         stats_dict = stats_df.iloc[0].to_dict()
#         # Remove NaN values
#         stats = {k: v for k, v in stats_dict.items() if pd.notna(v)}
#         return stats
    
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error generating log statistics: {str(e)}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"Statistics generation failed: {str(e)}"
#         )
 
