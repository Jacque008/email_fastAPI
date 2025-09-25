from typing import Optional, Union, List, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Depends, Form
from fastapi.templating import Jinja2Templates
import os
import pandas as pd

from ..schemas.summary import SummaryIn, SummaryOutWeb, SummaryOutAPI
from ..dataset.summary_dataset import SummaryDataset
from ..services.services import DefaultServices
from ..core.auth import get_current_user

# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/summary")
async def summary_page(request: Request, user=Depends(get_current_user)):
    """Summary page - GET"""
    return templates.TemplateResponse("summary.html", {"request": request})

@router.post("/summary")
async def process_summary_request(
    request: Request,
    email_id: Optional[str] = Form(None),
    errand_number: Optional[str] = Form(None),
    reference: Optional[str] = Form(None),
    user=Depends(get_current_user)
):
    """Process summary request from web form"""
    try:
        email_id_int = None
        if email_id and email_id.strip():
            try:
                email_id_int = int(email_id)
            except ValueError:
                return templates.TemplateResponse("summary.html", {
                    "request": request,
                    "error": "Email ID must be a valid number"
                })

        errand_number_clean = errand_number.strip() if errand_number and errand_number.strip() else None
        reference_clean = reference.strip() if reference and reference.strip() else None

        if not any([email_id_int, errand_number_clean, reference_clean]):
            return templates.TemplateResponse("summary.html", {
                "request": request,
                "error": "Please provide at least one identifier: Email ID, Errand Number, or Reference"
            })

        summary_request = SummaryIn(
            emailId=email_id_int,
            errandNumber=errand_number_clean,
            reference=reference_clean
        )

        summary_df = pd.DataFrame([{
            'emailId': summary_request.emailId,
            'errandNumber': summary_request.errandNumber,
            'reference': summary_request.reference
        }])
        ds = SummaryDataset(df=summary_df)
        result_df = ds.do_summary(use_case='webService')

        if result_df.empty:
            result = SummaryOutWeb(error_message="No summary generated")
        else:
            result_data = result_df.iloc[0].to_dict()
            clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}
            result = SummaryOutWeb(**clean_data)

        stats_df = ds.get_statistics()
        stats = stats_df.iloc[0].to_dict() if not stats_df.empty else {'has_data': False}
        stats = {k: v for k, v in stats.items() if k not in ['emailId', 'errandNumber', 'reference'] and pd.notna(v)}

        return templates.TemplateResponse("summary.html", {
            "request": request,
            "result": result.model_dump(),
            "statistics": stats,
            "input_params": {
                "email_id": email_id_int,
                "errand_number": errand_number_clean,
                "reference": reference_clean
            }
        })
        
    except ValueError as e:
        return templates.TemplateResponse("summary.html", {
            "request": request,
            "error": f"Invalid input: {str(e)}"
        })
    except Exception as e:
        return templates.TemplateResponse("summary.html", {
            "request": request,
            "error": f"Summary generation failed: {str(e)}"
        })

@router.post("/summary_api", response_model=List[SummaryOutAPI])
async def summary_api(summary_data: List[Dict[str, Any]]) -> List[SummaryOutAPI]:
    """
    API endpoint for generating summaries - accepts JSON array format

    Expected format: [
        {
            "emailId": 123,
            "errandNumber": "ER001",
            "reference": "REF123"
        },
        ...
    ]

    Args:
        summary_data: List[Dict] with emailId, errandNumber, or reference

    Returns:
        List[SummaryOutAPI]
    """
    try:
        if not summary_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Summary data cannot be empty"
            )

        summary_requests = []
        for i, data in enumerate(summary_data):
            try:
                summary_requests.append(SummaryIn(**data))
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid summary data in item {i}: {str(e)}"
                )

        summary_df = pd.DataFrame([{
            'emailId': sr.emailId,
            'errandNumber': sr.errandNumber,
            'reference': sr.reference
        } for sr in summary_requests])
        ds = SummaryDataset(df=summary_df)
        result_df = ds.do_summary(use_case='api')

        if result_df.empty:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="No summaries generated"
            )

        results = []
        for _, row in result_df.iterrows():
            result_data = row.to_dict()
            clean_data = {k: v for k, v in result_data.items() if pd.notna(v)}

            if clean_data.get('error_message'):
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=clean_data['error_message']
                )

            api_data = {
                'Summary_Combined_Info': clean_data.get('summary_combined', ''),
                'Error_Combined_Info': clean_data.get('error_combined', '')
            }
            results.append(SummaryOutAPI(**api_data))

        return results

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Summary generation failed: {str(e)}"
        )

@router.post("/summary_stats")
async def summary_statistics(
    summary_data: List[Dict[str, Any]]
):
    """
    Get statistics about available data for summary generation - accepts JSON array format

    Expected format: [
        {
            "emailId": 123,
            "errandNumber": "ER001",
            "reference": "REF123"
        },
        ...
    ]

    Args:
        summary_data: List[Dict] with emailId, errandNumber, or reference

    Returns:
        List of statistics about available data
    """
    try:
        if not summary_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Summary data cannot be empty"
            )

        summary_requests = []
        for i, data in enumerate(summary_data):
            try:
                summary_requests.append(SummaryIn(**data))
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid summary data in item {i}: {str(e)}"
                )

        summary_df = pd.DataFrame([{
            'emailId': sr.emailId,
            'errandNumber': sr.errandNumber,
            'reference': sr.reference
        } for sr in summary_requests])
        ds = SummaryDataset(df=summary_df)
        stats_df = ds.get_statistics()

        if stats_df.empty:
            return [{'has_data': False}] * len(summary_requests)

        results = []
        for _, row in stats_df.iterrows():
            stats_dict = row.to_dict()
            stats = {k: v for k, v in stats_dict.items()
                    if k not in ['emailId', 'errandNumber', 'reference'] and pd.notna(v)}
            results.append(stats)

        return results

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Statistics generation failed: {str(e)}"
        )