from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Form, Depends
from fastapi.templating import Jinja2Templates
import os
import pandas as pd
from ..schemas.forward import ForwardingIn, ForwardingOut
from ..dataset.forward_dataset import ForwardDataset
from ..services.services import DefaultServices
from ..services.utils import dataframe_to_model
from ..core.auth import get_current_user

# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/forward")
async def forward_page(request: Request, user=Depends(get_current_user)):
    """Email forwarding page - GET"""
    return templates.TemplateResponse("forward.html", {"request": request})

@router.post("/forward")
async def process_forward_email(
    request: Request,
    id: Optional[int] = Form(None),
    userId: Optional[int] = Form(None),
    user=Depends(get_current_user)
):
    """Process email forwarding form submission"""
    try:
        if id is None:
            return templates.TemplateResponse("forward.html", {
                "request": request,
                "error_message": "Email ID is required"
            })
        
        forwarding_request = ForwardingIn(
            id=id,
            userId=userId
        )
        
        # Use pure DataFrame approach - API handles all Pydantic conversions
        forwarding_df = pd.DataFrame([{
            'id': forwarding_request.id,
            'userId': forwarding_request.userId
        }])
        ds = ForwardDataset(df=forwarding_df, services=DefaultServices())
        result_df = ds.do_forward()

        if result_df.empty:
            forward = ForwardingOut(id=forwarding_request.id)
        else:
            result_data = result_df.iloc[0].to_dict()
            clean_data = {k: v for k, v in result_data.items() if pd.notna(v) and v != ''}
            clean_data.setdefault('id', forwarding_request.id)
            clean_data.setdefault('forward_address', '')
            clean_data.setdefault('forward_subject', '')
            clean_data.setdefault('forward_text', '')
            forward = ForwardingOut(**clean_data)
        
        return templates.TemplateResponse("forward.html", {
            "request": request,
            "result": forward.to_dict(),  # Convert to dict with correct field names
            "id": id,
            "userId": userId
        })
        
    except Exception as e:
        return templates.TemplateResponse("forward.html", {
            "request": request,
            "error_message": f"Processing failed: {str(e)}",
            "id": id,
            "userId": userId
        })     

@router.post("/forward_api", response_model=List[ForwardingOut])
async def forwarding_api(
    forwarding_data: List[Dict[str, Any]]
) -> List[ForwardingOut]:
    """API endpoint for email forwarding - accepts JSON array format

    Expected format:
    [{\"id\": 123, \"userId\": 456}, {\"id\": 789, \"userId\": 101}]
    """
    try:
        if not forwarding_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Forwarding data cannot be empty"
            )

        # Validate and convert to DataFrame
        forwarding_requests = []
        for i, data in enumerate(forwarding_data):
            id = data.get("id")
            if id is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required field 'id' in item {i}"
                )

            try:
                forwarding_request = ForwardingIn(
                    id=int(id),
                    userId=data.get("userId")
                )
                forwarding_requests.append(forwarding_request)
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid forwarding data in item {i}: {str(e)}"
                )

        # Convert to DataFrame for processing
        forwarding_df = pd.DataFrame([{
            'id': fr.id,
            'userId': fr.userId
        } for fr in forwarding_requests])

        # Process forwarding
        ds = ForwardDataset(df=forwarding_df, services=DefaultServices())
        ds.enrich_email_data()

        # Process forwarding (handles both single and batch)
        result_df = ds.do_forward()

        # Convert DataFrame results to Pydantic models
        results = []
        if not result_df.empty:
            for _, row in result_df.iterrows():
                result_data = row.to_dict()
                clean_data = {k: v for k, v in result_data.items() if pd.notna(v) and v != ''}
                # Ensure required fields exist
                clean_data.setdefault('forward_address', '')
                clean_data.setdefault('forward_subject', '')
                clean_data.setdefault('forward_text', '')
                results.append(ForwardingOut(**clean_data))
        else:
            # Return default results if processing failed
            results = [ForwardingOut(id=fr.id) for fr in forwarding_requests]

        return results
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Forwarding processing failed: {str(e)}"
        )