from typing import Optional, Union, List
from fastapi import APIRouter, Request, HTTPException, status, Depends, Form
from fastapi.templating import Jinja2Templates
import os

from ..schemas.summary import SummaryIn, SummaryOutWeb, SummaryOutAPI
from ..dataset.summary_dataset import SummaryDataset
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

@router.post("/summary", response_model=SummaryOutWeb)
async def process_summary_request(
    request: Request,
    email_id: Optional[str] = Form(None),
    errand_number: Optional[str] = Form(None),
    reference: Optional[str] = Form(None),
    user=Depends(get_current_user)
):
    """Process summary request from web form"""
    try:
        # Convert empty strings to None and parse email_id as int if provided
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
        
        dataset = SummaryDataset()
        result = dataset.generate_summary(summary_request, use_case='webService')

        stats = dataset.get_summary_statistics(summary_request)
        
        # Convert result to dict for template
        result_dict = result.model_dump()
        
        return templates.TemplateResponse("summary.html", {
            "request": request,
            "result": result_dict,
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

@router.post("/summary_api", response_model=Union[SummaryOutAPI, List[SummaryOutAPI]])
async def summary_api(
    summary_request: Union[SummaryIn, List[SummaryIn]],
    use_case: str = 'api'
) -> Union[SummaryOutAPI, List[SummaryOutAPI]]:
    """
    API endpoint for generating summaries - accepts single object or array

    Args:
        summary_request: SummaryIn object or List[SummaryIn] with emailId, errandNumber, or reference
        use_case: 'api' for combined summary only, 'webService' for detailed summaries

    Returns:
        SummaryOutAPI object or List[SummaryOutAPI] with only Summary_Combined_Info and Error_Combined_Info fields
    """
    try:
        dataset = SummaryDataset()
        
        # Handle both single object and array inputs
        if isinstance(summary_request, list):
            results = []
            for req in summary_request:
                result = dataset.generate_summary(req, use_case=use_case)
                if result.error_message:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=result.error_message
                    )
                api_result = dataset.convert_to_api_format(result)
                results.append(api_result)
            return results
        else:
            # Single object
            result = dataset.generate_summary(summary_request, use_case=use_case)
            if result.error_message:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=result.error_message
                )
            return dataset.convert_to_api_format(result)
        
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
    summary_request: Union[SummaryIn, List[SummaryIn]]
):
    """
    Get statistics about available data for summary generation - accepts single object or array
    
    Args:
        summary_request: SummaryIn object or List[SummaryIn] with emailId, errandNumber, or reference
        
    Returns:
        Statistics about available data (single dict or list of dicts)
    """
    try:
        dataset = SummaryDataset()
        
        # Handle both single object and array inputs
        if isinstance(summary_request, list):
            results = []
            for req in summary_request:
                stats = dataset.get_summary_statistics(req)
                results.append(stats)
            return results
        else:
            # Single object
            stats = dataset.get_summary_statistics(summary_request)
            return stats
        
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