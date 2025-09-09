from typing import List, Optional, Union, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Form, Depends, UploadFile, File
from fastapi.templating import Jinja2Templates
import os
import json
from ..schemas.payment import PaymentIn, PaymentOut
from ..dataset.payment_dataset import PaymentDataset
from .deps import get_current_active_user

# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/payment")
async def payment_page(request: Request, user=Depends(get_current_active_user)):
    """Payment matching page - GET"""
    return templates.TemplateResponse("payment.html", {"request": request})

@router.post("/payment")
async def process_payment_matching(
    request: Request,
    payment_file: UploadFile = File(...),
    user=Depends(get_current_active_user)
):
    """Process payment matching from uploaded JSON file"""
    try:
        # Validate file type
        if not payment_file.filename.endswith('.json'):
            return templates.TemplateResponse("payment.html", {
                "request": request,
                "error": "Please upload a JSON file"
            })
        
        # Read and parse JSON file
        try:
            file_content = await payment_file.read()
            payments_data = json.loads(file_content.decode('utf-8'))
        except json.JSONDecodeError as e:
            return templates.TemplateResponse("payment.html", {
                "request": request,
                "error": f"Invalid JSON format: {str(e)}"
            })
        except Exception as e:
            return templates.TemplateResponse("payment.html", {
                "request": request,
                "error": f"Error reading file: {str(e)}"
            })
        
        # Convert to list if single payment
        if isinstance(payments_data, dict):
            payments_data = [payments_data]
        elif not isinstance(payments_data, list):
            return templates.TemplateResponse("payment.html", {
                "request": request,
                "error": "JSON data must be an object or array of payment objects"
            })
        
        # Validate and create PaymentIn objects
        payment_objects = []
        for i, payment_data in enumerate(payments_data):
            try:
                payment_objects.append(PaymentIn(**payment_data))
            except Exception as e:
                # Show available fields for debugging
                available_fields = list(payment_data.keys()) if isinstance(payment_data, dict) else "Not a dict"
                sample_data = str(payment_data)[:300] if payment_data else "Empty"
                return templates.TemplateResponse("payment.html", {
                    "request": request,
                    "error": f"Invalid payment data in item {i+1}: {str(e)}\n\nAvailable fields: {available_fields}\n\nSample data: {sample_data}..."
                })
        
        # Process payments using PaymentDataset
        ds = PaymentDataset()
        results = ds.match_payments(payment_objects)
        
        # Get statistics
        statistics = ds.matching_statistics(payment_objects)
        
        return templates.TemplateResponse("payment.html", {
            "request": request,
            "results": [result.model_dump() for result in results],
            "total_processed": len(results),
            "statistics": statistics
        })
        
    except Exception as e:
        return templates.TemplateResponse("payment.html", {
            "request": request,
            "error": f"Processing failed: {str(e)}"
        })

@router.post("/payment_api", response_model=Union[PaymentOut, List[PaymentOut]])
async def payment_matching_api(
    payment_data: Union[Dict[str, Any], List[Dict[str, Any]]],
    user=Depends(get_current_active_user)
) -> Union[PaymentOut, List[PaymentOut]]:
    """API endpoint for payment matching - accepts JSON with payment data format
    
    Expected format:
    Single payment: {
        "id": 44353,
        "amount": 113700,
        "reference": "1000522704", 
        "info": "SKADEUTBETALNING...",
        "bankName": "Agria Djurförsäkring",
        "createdAt": "2024-11-05 13:49:18.137 +0100"
    }
    
    Multiple payments: [payment1, payment2, ...]
    """
    try:
        # Handle single request
        if isinstance(payment_data, dict):
            payment_request = PaymentIn(**payment_data)
            ds = PaymentDataset()
            result = ds.match_payments([payment_request])
            return result[0] if result else PaymentOut(
                id=payment_request.id,
                amount=f"{payment_request.amount / 100:.2f} kr",
                bankName=payment_request.bankName,
                createdAt=payment_request.createdAt,
                status="Not Found"
            )
        
        # Handle batch requests
        elif isinstance(payment_data, list):
            payment_requests = []
            for i, data in enumerate(payment_data):
                try:
                    payment_requests.append(PaymentIn(**data))
                except Exception as e:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid payment data in item {i}: {str(e)}"
                    )
            
            ds = PaymentDataset()
            results = ds.match_payments(payment_requests)
            return results
        
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid request format. Expected JSON object or array with payment data"
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Payment matching failed: {str(e)}"
        )