from typing import List, Optional, Union, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Form, Depends
from fastapi.templating import Jinja2Templates
import os
from ..schemas.forward import ForwardingIn, ForwardingOut
from ..dataset.forwarding_dataset import ForwardingEmailDataset
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
        
        # Use ForwardingEmailDataset directly
        ds = ForwardingEmailDataset(services=DefaultServices())
        forward = ds.do_forwarding(forwarding_request)
        
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

@router.post("/forward_api", response_model=Union[ForwardingOut, List[ForwardingOut]])
async def forwarding_api(
    forwarding_data: Union[Dict[str, Any], List[Dict[str, Any]]]
) -> Union[ForwardingOut, List[ForwardingOut]]:
    """API endpoint for email forwarding - accepts JSON with {id, userId} format"""
    try:
        # Handle single request
        if isinstance(forwarding_data, dict):
            id = forwarding_data.get("id")
            if id is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Missing required field 'id'"
                )

            forwarding_request = ForwardingIn(
                id=int(id),
                userId=forwarding_data.get("userId")
            )
            ds = ForwardingEmailDataset(services=DefaultServices())
            result = ds.do_forwarding(forwarding_request)
            return result
        
        # Handle batch requests
        elif isinstance(forwarding_data, list):
            results = []
            for i, data in enumerate(forwarding_data):
                id = data.get("id")
                if id is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Missing required field 'id' in item {i}"
                    )
                
                forwarding_request = ForwardingIn(
                    id=int(id),
                    userId=data.get("userId")
                )
                ds = ForwardingEmailDataset(services=DefaultServices())
                result = ds.do_forwarding(forwarding_request)
                results.append(result)
            return results
        
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid request format. Expected JSON object or array with {id, userId}"
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Forwarding processing failed: {str(e)}"
        )