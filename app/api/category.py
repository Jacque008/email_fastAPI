from typing import List
from fastapi import APIRouter, Request, HTTPException, status, UploadFile, File, Depends
from fastapi.templating import Jinja2Templates
import pandas as pd
import json
import os
from ..schemas.email import EmailIn, EmailOut
from ..dataset.email_dataset import EmailDataset
from ..services.services import DefaultServices
from ..core.auth import get_current_user

# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/category")
async def category_page(request: Request, user=Depends(get_current_user)):
    """Email categorization page - GET"""
    return templates.TemplateResponse("category.html", {"request": request})

@router.post("/category")
async def process_category_emails(
    request: Request,
    emailJsonFile: UploadFile = File(...),
    user=Depends(get_current_user)):
    """Process uploaded email JSON file and return categorized results"""
    try:
        if not (emailJsonFile.filename and emailJsonFile.filename.endswith('.json')):
            return templates.TemplateResponse("category.html", {
                "request": request,
                "error_message": "Only JSON files are allowed"
            })
        
        content = await emailJsonFile.read()
        try:
            email_data = json.loads(content.decode('utf-8'))
        except json.JSONDecodeError:
            return templates.TemplateResponse("category.html", {
                "request": request,
                "error_message": "Invalid JSON format"
            })
        
        if not isinstance(email_data, list):
            return templates.TemplateResponse("category.html", {
                "request": request,
                "error_message": "JSON file should contain a list of emails"
            })
        
        if len(email_data) == 0:
            return templates.TemplateResponse("category.html", {
                "request": request,
                "record_json": [],
                "message": "No emails found in the uploaded file"
            })
        
        emails = []
        for i, email_dict in enumerate(email_data):
            try:
                email_obj = EmailIn(**email_dict)
                emails.append(email_obj)
            except Exception as e:
                return templates.TemplateResponse("category.html", {
                    "request": request,
                    "error_message": f"Invalid email data at index {i}: {str(e)}"
                })
        
        try:
            email_df = pd.DataFrame([e.model_dump(by_alias=True) for e in emails])            
            ds = EmailDataset(df=email_df, services=DefaultServices())
            processed_df = ds.do_connect()

        except Exception as debug_error:
            import traceback
            traceback.print_exc()
            raise debug_error
        
        try:
            cleaned_df = processed_df.copy()
            
            # Fill numeric columns with None (which becomes null in JSON)
            numeric_columns = cleaned_df.select_dtypes(include=['float64', 'Float64', 'int64', 'Int64']).columns
            for col in numeric_columns:
                cleaned_df[col] = cleaned_df[col].where(pd.notna(cleaned_df[col]), None)
            
            # Fill string/object columns with empty string
            string_columns = cleaned_df.select_dtypes(include=['object', 'string']).columns
            for col in string_columns:
                cleaned_df[col] = cleaned_df[col].fillna("")
            
            # Fill boolean columns with False
            bool_columns = cleaned_df.select_dtypes(include=['bool']).columns
            for col in bool_columns:
                cleaned_df[col] = cleaned_df[col].fillna(False)
                
            rows = cleaned_df.to_dict(orient="records")
            
        except Exception as convert_error:
            import traceback
            traceback.print_exc()
            raise convert_error
        
        # Clean up the data for template display - simplified since DataFrame is already properly processed
        processed_emails = []
        try:
            for i, row in enumerate(rows):
                cleaned_row = {}
                for key, value in row.items():
                    # Simple cleanup - just convert to string for HTML display
                    if value is None:
                        cleaned_row[key] = ""
                    else:
                        # Convert to string but preserve array format for display
                        if isinstance(value, list) and len(value) > 0:
                            cleaned_row[key] = str(value)
                        else:
                            cleaned_row[key] = str(value) if value != "" else ""
                processed_emails.append(cleaned_row)
        except Exception as cleanup_error:
            import traceback
            traceback.print_exc()
            raise cleanup_error
        
        # Generate and display statistics
        stats_data = None
        try:
            stats_data = ds.classifier.statistic(processed_df)
        except Exception as stats_error:
            import traceback
            traceback.print_exc()
            raise Exception(f"Statistics failed: {str(stats_error)}")

        return templates.TemplateResponse("category.html", {
            "request": request,
            "record_json": processed_emails,
            "filename": emailJsonFile.filename,
            "stats_data": stats_data
        })
    
    except Exception as e:
        return templates.TemplateResponse("category.html", {
            "request": request,
            "error_message": f"Processing failed: {str(e)}"
        })

@router.post("/category_api", response_model=List[EmailOut])
async def category_api(emails: List[EmailIn]):
    try:
        print("🔍 Step 1: Creating DataFrame from input emails")
        email_df = pd.DataFrame([e.model_dump(by_alias=True) for e in emails])
        print(f"📊 DataFrame created with {len(email_df)} rows")

        print("🔍 Step 2: Creating EmailDataset and DefaultServices")
        ds = EmailDataset(df=email_df, services=DefaultServices())
        print("✅ EmailDataset and services initialized")

        print("🔍 Step 3: Running do_connect processing pipeline")
        processed_df = ds.do_connect()
        print("✅ Processing pipeline completed")
        
        # Use the same DataFrame processing as web interface
        cleaned_df = processed_df.copy()
        
        # Fill numeric columns with None (which becomes null in JSON)
        numeric_columns = cleaned_df.select_dtypes(include=['float64', 'Float64', 'int64', 'Int64']).columns
        for col in numeric_columns:
            cleaned_df[col] = cleaned_df[col].where(pd.notna(cleaned_df[col]), None)
        
        # Fill string/object columns with empty string
        string_columns = cleaned_df.select_dtypes(include=['object', 'string']).columns
        for col in string_columns:
            cleaned_df[col] = cleaned_df[col].fillna("")
        
        # Fill boolean columns with False
        bool_columns = cleaned_df.select_dtypes(include=['bool']).columns
        for col in bool_columns:
            cleaned_df[col] = cleaned_df[col].fillna(False)
            
        rows = cleaned_df.to_dict(orient="records")
        
        return rows
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Email processing failed: {str(e)}"
        )