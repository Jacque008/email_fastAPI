from typing import List, Optional, Union
from fastapi import FastAPI, Request, Depends, HTTPException, status, UploadFile, File, Form
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.sessions import SessionMiddleware
from authlib.integrations.starlette_client import OAuth
import os
import pandas as pd
import jwt
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from .services.utils import fetchFromDB
from .schemas.email import EmailIn, EmailOut
from .schemas.fw_email import ForwardingIn, ForwardingOut
from .workflow.create_forwarding import create_forwarding_workflow
from .workflow.categorize_connect import CategoryConnect
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# # Import new routers
# from ..spare.api_routes import router as api_router
# from ..spare.web_routes import router as web_router

load_dotenv()

# Get project root directory path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

app = FastAPI(
    title="DRP Email Processing with Google Auth", 
    version="1.0.0",
    description="Email processing API with Google OAuth authentication"
)
templates = Jinja2Templates(directory=TEMPLATES_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SECRET_KEY"),
    same_site="lax", 
    https_only=False,  # Use HTTP for local development environment
    max_age=3600,  # Session expires in 1 hour
    session_cookie="session"  # Explicit session cookie name
)

# Include new routers
# app.include_router(api_router)
# app.include_router(web_router)

oauth = OAuth()
oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
    authorize_params=None,
    access_token_url="https://oauth2.googleapis.com/token",
    access_token_params=None,
    refresh_token_url=None,
    jwks_uri="https://www.googleapis.com/oauth2/v3/certs",
    client_kwargs={
        "scope": "openid email profile"
    },
)

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

security = HTTPBearer(auto_error=False)

class AuthService:
    @staticmethod
    def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS)
        
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
        return encoded_jwt
    
    @staticmethod
    def verify_token(token: str):
        """Verify JWT token"""
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired"
            )
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate token"
            )
    
    @staticmethod
    def check_email_authorization(email: str, custom_query: str = None) -> bool:
        """
        Check if email is in whitelist
        
        Args:
            email: Email address to check
            custom_query: Custom query statement, use default query if not provided
        
        Returns:
            bool: Return True if email is in whitelist, otherwise False
        
        Raises:
            Exception: Throw exception when database query fails
        """
        try:
            # Use custom query or default query
            if custom_query:
                query = custom_query
            else:
                # Default query - please modify according to actual table structure
                query = "SELECT email FROM admin_user au"
            
            # Get authorized email list from database
            whitelist_df = fetchFromDB(query)
            if whitelist_df.empty:
                return False
            
            # Extract email list and convert to lowercase for comparison
            authorized_emails = whitelist_df['email'].str.lower().tolist()
            
            # Check if user email is in whitelist (case insensitive)
            return email.lower() in authorized_emails
            
        except Exception as e:
            # Log error and re-throw
            raise Exception(f"Database query failed: {str(e)}")

def get_current_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user (supports session and JWT)"""
    user = None

    # Check for test API key in development mode
    if os.getenv("ENV_MODE") == "local":
        api_key = request.headers.get("X-API-Key")
        if api_key == os.getenv("TEST_API_KEY", "test-key-123"):
            # Return a mock user for testing
            return {
                "sub": "test-user",
                "email": "test@example.com",
                "name": "Test User",
                "provider": "api-key"
            }

    # Check JWT token first
    if credentials:
        payload = AuthService.verify_token(credentials.credentials)
        user = payload.get("user")

    # Fallback to session
    if not user:
        user = request.session.get("user")
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user

def get_optional_current_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user (optional, no exception thrown)"""
    try:
        return get_current_user(request, credentials)
    except HTTPException:
        return None

@app.get("/")
async def root(request: Request):
    """Root path redirect to dashboard"""
    return RedirectResponse(url="/dashboard")

@app.get("/health")
def health_check():
    """Health check"""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc)}

# =============================================================================
# Authentication related routes
# =============================================================================

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/login/google")
async def login_with_google(request: Request):
    """Initiate Google OAuth login"""
    try:
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback")
        print(f"Debug: Using redirect URI: {redirect_uri}")  # Debug info
        print(f"Debug: Google Client ID: {os.getenv('GOOGLE_CLIENT_ID')[:10]}...")  # Show partial ID
        
        # Generate and store a custom state
        import secrets
        state = secrets.token_urlsafe(32)
        request.session["oauth_state"] = state
        print(f"Debug: Generated state: {state[:10]}...")  # Debug first 10 chars
        print(f"Debug: Session after storing state: {list(request.session.keys())}")  # Debug session contents
        
        return await oauth.google.authorize_redirect(request, redirect_uri, state=state)
    except Exception as e:
        print(f"Error in login_with_google: {str(e)}")  # Debug info
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initiate Google login: {str(e)}"
        )

@app.get("/auth/google/callback")
async def auth_google_callback(request: Request):
    """Handle Google OAuth callback"""
    try:
        print("Debug: Received callback from Google")  # Debug info
        print(f"Debug: Session keys at callback: {list(request.session.keys())}")  # Debug session state
        
        # Manual state verification
        received_state = request.query_params.get("state")
        stored_state = request.session.get("oauth_state")
        print(f"Debug: Received state: {received_state[:10] if received_state else None}...")
        print(f"Debug: Stored state: {stored_state[:10] if stored_state else None}...")
        
        # Verify our custom state
        if not received_state or received_state != stored_state:
            print("Debug: Custom state verification failed")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid state parameter"
            )
        
        # Manual token exchange instead of using authlib
        code = request.query_params.get("code")
        if not code:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing authorization code"
            )
        
        # Exchange code for token manually
        import httpx
        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback")
        }
        
        async with httpx.AsyncClient() as client:
            token_response = await client.post(token_url, data=token_data)
            token_response.raise_for_status()
            token = token_response.json()
        
        print(f"Debug: Token received: {bool(token)}")  # Debug info
        
        # Parse ID token for verified email information
        id_token = token.get("id_token")
        if id_token:
            # Decode ID token (Google's ID tokens are JWTs)
            import jwt
            try:
                # For Google ID tokens, we can decode without verification for getting basic claims
                # since we got it directly from Google's token endpoint
                claims = jwt.decode(id_token, options={"verify_signature": False})
                print(f"Debug: Claims from ID token: email={claims.get('email')}, email_verified={claims.get('email_verified')}")
            except Exception as e:
                print(f"Debug: Failed to decode ID token: {e}")
                # Fallback to userinfo endpoint
                userinfo_url = "https://www.googleapis.com/oauth2/v2/userinfo"
                headers = {"Authorization": f"Bearer {token['access_token']}"}
                
                async with httpx.AsyncClient() as client:
                    userinfo_response = await client.get(userinfo_url, headers=headers)
                    userinfo_response.raise_for_status()
                    claims = userinfo_response.json()
        else:
            # Fallback to userinfo endpoint if no ID token
            userinfo_url = "https://www.googleapis.com/oauth2/v2/userinfo"
            headers = {"Authorization": f"Bearer {token['access_token']}"}
            
            async with httpx.AsyncClient() as client:
                userinfo_response = await client.get(userinfo_url, headers=headers)
                userinfo_response.raise_for_status()
                claims = userinfo_response.json()
        
        print(f"Debug: Final claims: {claims}")  # Debug info
        
        if not claims:
            print("Debug: No claims found in token")  # Debug info
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get user information from Google"
            )
        
        email = claims.get("email")
        email_verified = claims.get("email_verified")
        sub = claims.get("sub")
        name = claims.get("name")
        picture = claims.get("picture")
        hd = claims.get("hd")  # Hosted domain (G Suite domain)
        
        print(f"Debug: User email: {email}, verified: {email_verified}, domain: {hd}")  # Debug info

        if not email:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No email address provided by Google"
            )
        
        # More lenient verification check:
        # - Accept if email_verified is True
        # - Accept if email_verified is None but user has a hosted domain (G Suite)
        # - Accept if email_verified is None and it's a gmail.com address (Google's own)
        is_verified = (
            email_verified is True or 
            (email_verified is None and hd) or  # G Suite domain
            (email_verified is None and email.endswith('@gmail.com'))  # Gmail address
        )
        
        if not is_verified:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Google email not verified"
            )
        
        # Check if email is in database whitelist
        try:
            is_authorized = AuthService.check_email_authorization(email)     
            if not is_authorized:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied: Your email address is not authorized to access this application. Please contact your administrator."
                )

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authorization service temporarily unavailable. Please try again later."
            )

        user_data = {
            "sub": sub,
            "email": email,
            "name": name,
            "picture": picture,
            "provider": "google",
            "authenticated_at": datetime.now(timezone.utc).isoformat()
        }

        request.session["user"] = user_data
        
        access_token = AuthService.create_access_token(
            data={"user": user_data}
        )
        
        # Redirect to dashboard after success instead of returning JSON
        return RedirectResponse(url="/dashboard", status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Authentication failed: {str(e)}"
        )

@app.post("/logout")
async def logout(request: Request, user=Depends(get_current_user)):
    """Clear user session"""
    request.session.clear()
    return {"message": "Successfully logged out"}

@app.get("/me")
async def get_current_user_info(user=Depends(get_current_user)):
    """Get current user info and generate fresh access token"""
    # Generate a fresh access token for API usage
    access_token = AuthService.create_access_token(data={"user": user})
    
    return {
        "user": user,
        "authenticated": True,
        "access_token": access_token,
        "token_type": "bearer",
        "usage": "Use this token in Postman Authorization header as 'Bearer <access_token>'"
    }

# =============================================================================
# statistics related routes
# =============================================================================

@app.get("/admin/stats")
async def get_stats(user=Depends(get_current_user)):
    return {
        "user_email": user.get("email"),
        "total_processed": 0,  
        "last_activity": datetime.now(timezone.utc),
        "note": "Statistics endpoint - implement according to your needs"
    }

# =============================================================================
# Email processing related routes (Legacy - kept for backward compatibility)
# =============================================================================

@app.get("/dashboard")
async def dashboard(request: Request, user=Depends(get_optional_current_user)):
    """Dashboard page"""
    return templates.TemplateResponse("dashboard.html", {
        "request": request, 
        "userId": user.get("name") if user else None
    })

@app.get("/category")
async def category_page(request: Request, user=Depends(get_current_user)):
    """Email categorization page - GET"""
    return templates.TemplateResponse("category.html", {"request": request})

@app.post("/category")
async def process_category_emails(
    request: Request,
    emailJsonFile: UploadFile = File(...),
    user=Depends(get_current_user)):
    """Process uploaded email JSON file and return categorized results"""
    try:
        if not emailJsonFile.filename.endswith('.json'):
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
        
        # Process emails using CategoryConnect (same logic as category_api)
        try:
            email_df = pd.DataFrame([e.model_dump(by_alias=True) for e in emails])
            cc = CategoryConnect(df=email_df)
            processed_df = cc.do_connect()

        except Exception as debug_error:
            error_msg = f"main: Detailed error in category processing: {str(debug_error)}"
            import traceback
            traceback.print_exc()

            with open('/tmp/category_debug.log', 'w') as f:
                f.write(f"{error_msg}\n")
                f.write("Full traceback:\n")
                traceback.print_exc(file=f)
            
            raise debug_error
        
        # Convert to records for template display
        try:
            rows = processed_df.where(pd.notna(processed_df), None).to_dict(orient="records")
        except Exception as convert_error:
            import traceback
            traceback.print_exc()
            raise convert_error
        
        # Clean up the data for template display
        processed_emails = []
        try:
            for i, row in enumerate(rows):
                cleaned_row = {}
                for k, v in row.items():
                    try:
                        if isinstance(v, list):
                            cleaned_row[k] = v
                        elif pd.isna(v):
                            cleaned_row[k] = None
                        elif isinstance(v, bool):
                            cleaned_row[k] = v  # Preserve boolean values
                        else:
                            cleaned_row[k] = str(v) if v is not None else None
                    except Exception as item_error:
                        print(f"Debug: Error processing key '{k}' with value '{v}' (type: {type(v)}): {str(item_error)}")
                        # Log the problematic value
                        if hasattr(v, 'size'):
                            print(f"Debug: Value has size attribute: {v.size}")
                        if hasattr(v, '__len__'):
                            print(f"Debug: Value has length: {len(v)}")
                        raise item_error
                
                processed_emails.append(cleaned_row)
            
        except Exception as cleanup_error:
            print(f"Debug: Error in data cleanup: {str(cleanup_error)}")
            import traceback
            traceback.print_exc()
            raise cleanup_error
        
        # Generate and display statistics
        stats_data = None
        try:
            stats_data = cc.classifier.statistic(processed_df)
        except Exception as stats_error:
            print(f"Debug: Error generating statistics: {str(stats_error)}")
        
        # Return template with results
        return templates.TemplateResponse("category.html", {
            "request": request,
            "record_json": processed_emails,
            "filename": emailJsonFile.filename,
            "total_processed": len(processed_emails),
            "success_message": f"Successfully processed {len(processed_emails)} emails",
            "statistics": stats_data
        })
        
    except Exception as e:
        print(f"Error processing category emails: {str(e)}")
        return templates.TemplateResponse("category.html", {
            "request": request,
            "error_message": f"Processing failed: {str(e)}"
        })

@app.post("/category_api", response_model=List[EmailOut])
async def category_api(
    emails: List[EmailIn], 
    user=Depends(get_current_user)):
    try:
        email_df = pd.DataFrame([e.model_dump(by_alias=True) for e in emails])
        cc = CategoryConnect(df=email_df)
        processed_df = cc.do_connect()
        rows = processed_df.where(pd.notna(processed_df), None).to_dict(orient="records")
        
        return rows
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Email processing failed: {str(e)}"
        )

@app.get("/forward")
async def forward_page(request: Request, user=Depends(get_current_user)):
    """Email forwarding page - GET"""
    return templates.TemplateResponse("forward.html", {"request": request})

@app.post("/forward")
async def process_forward_email(
    request: Request,
    emailId: Optional[int] = Form(None),
    recipient: Optional[str] = Form(None),
    correctedCategory: Optional[str] = Form(None),
    userId: Optional[int] = Form(None),
    user=Depends(get_current_user)
):
    """Process single email forwarding via form submission"""
    try:
        # Validate required fields
        if not emailId:
            return templates.TemplateResponse("forward.html", {
                "request": request,
                "error_message": "Email ID is required"
            })
        
        if not recipient:
            return templates.TemplateResponse("forward.html", {
                "request": request,
                "error_message": "Recipient is required",
                "id": emailId
            })
            
        if not correctedCategory:
            return templates.TemplateResponse("forward.html", {
                "request": request,
                "error_message": "Corrected category is required",
                "id": emailId,
                "recipient": recipient
            })

        # Create forwarding request
        forwarding_request = ForwardingIn(
            email_id=emailId,
            recipient=recipient,
            corrected_category=correctedCategory,
            user_id=userId
        )

        # Process forwarding using workflow
        workflow = create_forwarding_workflow()
        result = workflow.do_single_forwarding(forwarding_request)

        # Check for errors
        if result.error_message:
            return templates.TemplateResponse("forward.html", {
                "request": request,
                "error_message": result.error_message,
                "id": emailId,
                "recipient": recipient,
                "correctedCategory": correctedCategory,
                "userId": userId
            })

        # Success case
        return templates.TemplateResponse("forward.html", {
            "request": request,
            "result": result.to_dict(),
            "success_message": f"Successfully processed forwarding for Email ID: {emailId}",
            "id": emailId,
            "recipient": recipient,
            "correctedCategory": correctedCategory,
            "userId": userId
        })

    except Exception as e:
        print(f"Error in process_forward_email: {str(e)}")
        return templates.TemplateResponse("forward.html", {
            "request": request,
            "error_message": f"Processing failed: {str(e)}",
            "id": emailId,
            "recipient": recipient,
            "correctedCategory": correctedCategory,
            "userId": userId
        })     
        
        
@app.post("/forward_api")
async def forwarding_api(
    forwarding_data: Union[ForwardingIn, List[ForwardingIn]],
    user=Depends(get_current_user)
) -> Union[ForwardingOut, List[ForwardingOut]]:
    """API endpoint for email forwarding - supports both single and batch processing"""
    try:
        workflow = create_forwarding_workflow()
        
        # Handle single request
        if isinstance(forwarding_data, ForwardingIn):
            result = workflow.do_single_forwarding(forwarding_data)
            return result
        
        # Handle batch requests
        elif isinstance(forwarding_data, list):
            results = workflow.do_batch_forwarding(forwarding_data)
            return results
        
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid request format. Expected ForwardingIn or List[ForwardingIn]"
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Forwarding processing failed: {str(e)}"
        )

@app.get("/logout")
async def logout_page(request: Request):
    """Logout page"""
    request.session.clear()
    return RedirectResponse(url="/login")

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "message": exc.detail,
            "status_code": exc.status_code
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
