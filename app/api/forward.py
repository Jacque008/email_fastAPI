from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Form, Depends
from fastapi.templating import Jinja2Templates
import os
import pandas as pd
from ..schemas.forward import ForwardingIn, ForwardingOut
from ..dataset.forward_dataset import ForwardDataset
from ..services.services import DefaultServices
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

            if 'error' in result_data and pd.notna(result_data['error']):
                forward = ForwardingOut(
                    id=result_data.get('id', forwarding_request.id),
                    error=result_data['error']
                )
            else:
                clean_data = {k: v for k, v in result_data.items() if pd.notna(v) and v != ''}
                clean_data.setdefault('id', forwarding_request.id)
                clean_data.setdefault('action', '')
                clean_data.setdefault('forward_address', '')
                clean_data.setdefault('forward_subject', '')
                clean_data.setdefault('forward_text', None)
                clean_data.setdefault('journal_data', None)
                forward = ForwardingOut(**clean_data)

        result_dict = forward.to_dict()
        filtered_result = {k: v for k, v in result_dict.items() if v is not None and v != ''}

        return templates.TemplateResponse("forward.html", {
            "request": request,
            "result": filtered_result,
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

@router.post("/forward_api", response_model=List[Dict[str, Any]])
async def forwarding_api(
    forwarding_data: List[ForwardingIn]
) -> List[Dict[str, Any]]:
    """API endpoint for email forwarding - accepts JSON array with single record

    Expected format:
    [{\"id\": 123, \"userId\": 456}]
    """
    try:
        if not forwarding_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Forwarding data cannot be empty. Expected exactly one record."
            )

        if len(forwarding_data) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"API accepts only one record at a time. Received {len(forwarding_data)} records."
            )

        forwarding_df = pd.DataFrame([fw.model_dump(by_alias=True) for fw in forwarding_data])
        ds = ForwardDataset(df=forwarding_df, services=DefaultServices())
        result_df = ds.do_forward()

        if result_df.empty:
            forward = ForwardingOut(id=forwarding_data[0].id)
            result_dict = forward.to_dict()
            filtered_result = {k: v for k, v in result_dict.items() if v is not None}
            return [filtered_result]
        else:
            result_data = result_df.iloc[0].to_dict()

            if 'error' in result_data and pd.notna(result_data['error']):
                forward = ForwardingOut(
                    id=result_data.get('id', forwarding_data[0].id),
                    error=result_data['error']
                )
            else:
                clean_data = {k: v for k, v in result_data.items() if pd.notna(v) and v != ''}
                clean_data.setdefault('action', '')
                clean_data.setdefault('forward_address', '')
                clean_data.setdefault('forward_subject', '')
                clean_data.setdefault('forward_text', None)
                clean_data.setdefault('journal_data', None)
                forward = ForwardingOut(**clean_data)

            result_dict = forward.to_dict()
            filtered_result = {k: v for k, v in result_dict.items() if v is not None and v != ''}
            return [filtered_result]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Forwarding processing failed: {str(e)}"
        )