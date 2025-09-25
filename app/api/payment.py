from typing import List, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Form, Depends, UploadFile, File
from fastapi.templating import Jinja2Templates
import os
import json
import pandas as pd
from ..schemas.payment import PaymentIn, PaymentOut
from ..dataset.payment_dataset import PaymentDataset
from ..core.auth import get_current_user

# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/payment")
async def payment_page(request: Request, user=Depends(get_current_user)):
    """Payment matching page - GET"""
    return templates.TemplateResponse("payment.html", {"request": request})

@router.post("/payment")
async def process_payment_matching(
    request: Request,
    payment_file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    """Process payment matching from uploaded JSON file"""
    try:
        if payment_file.filename and not payment_file.filename.endswith('.json'):
            return templates.TemplateResponse("payment.html", {
                "request": request,
                "error": "Please upload a JSON file"
            })
        
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
                available_fields = list(payment_data.keys()) if isinstance(payment_data, dict) else "Not a dict"
                sample_data = str(payment_data)[:300] if payment_data else "Empty"
                return templates.TemplateResponse("payment.html", {
                    "request": request,
                    "error": f"Invalid payment data in item {i+1}: {str(e)}\n\nAvailable fields: {available_fields}\n\nSample data: {sample_data}..."
                })
        
        payment_df = pd.DataFrame([{
            'id': p.id,
            'amount': p.amount,
            'reference': p.reference,
            'info': p.info,
            'bankName': p.bankName,
            'createdAt': p.createdAt
        } for p in payment_objects])
        ds = PaymentDataset(df=payment_df)
        result_df = ds.do_match()

        results = []
        if not result_df.empty:
            for _, row in result_df.iterrows():
                result_data = row.to_dict()
                clean_data = {k: v for k, v in result_data.items() if not (pd.isna(v) if not isinstance(v, (list, dict)) else False)}
                results.append(PaymentOut(**clean_data))

        statistics = ds.get_statistics()
        
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

@router.post("/payment_api", response_model=List[PaymentOut])
async def payment_matching_api(
    payment_data: List[Dict[str, Any]]
) -> List[PaymentOut]:
    """API endpoint for payment matching - accepts JSON array format

    Expected format:
    [{"id": 44353, "amount": 113700, "reference": "1000522704", "info": "SKADEUTBETALNING...", "bankName": "Agria Djurförsäkring", "createdAt": "2024-11-05 13:49:18.137 +0100"}]
    """
    try:
        if not payment_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Payment data cannot be empty"
            )

        payment_requests = []
        for i, data in enumerate(payment_data):
            try:
                payment_requests.append(PaymentIn(**data))
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid payment data in item {i}: {str(e)}"
                )

        payment_df = pd.DataFrame([{
            'id': p.id,
            'amount': p.amount,
            'reference': p.reference,
            'info': p.info,
            'bankName': p.bankName,
            'createdAt': p.createdAt
        } for p in payment_requests])

        # Process payments
        ds = PaymentDataset(df=payment_df)
        result_df = ds.do_match()

        results = []
        if not result_df.empty:
            for _, row in result_df.iterrows():
                result_data = row.to_dict()
                clean_data = {k: v for k, v in result_data.items() if not (pd.isna(v) if not isinstance(v, (list, dict)) else False)}
                results.append(PaymentOut(**clean_data))

        return results
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Payment matching failed: {str(e)}"
        )