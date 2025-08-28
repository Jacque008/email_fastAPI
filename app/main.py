from typing import List, Optional
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
from .workflow.categorize_connect import CategoryConnect
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Import new routers
from ..spare.api_routes import router as api_router
from ..spare.web_routes import router as web_router

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
    https_only=False  # Use HTTP for local development environment, set to False
)

# Include new routers
app.include_router(api_router)
app.include_router(web_router)

oauth = OAuth()
oauth.register(
    name="google",
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    client_kwargs={"scope": "openid email profile"},
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
                query = "SELECT id, email FROM admin_user au"
            
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
        
        return await oauth.google.authorize_redirect(request, redirect_uri)
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
        
        token = await oauth.google.authorize_access_token(request)
        print(f"Debug: Token received: {bool(token)}")  # Debug info

        claims = token.get("userinfo")
        if not claims and "id_token" in token:
            claims = oauth.google.parse_id_token(request, token)
        
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
        
        print(f"Debug: User email: {email}, verified: {email_verified}")  # Debug info

        if not email or not email_verified:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Google email not verified"
            )
        
        # Check if email is in database whitelist
        try:
            print(f"Debug: Checking email authorization for: {email}")  # Debug info
            is_authorized = AuthService.check_email_authorization(email)     
            if not is_authorized:
                print(f"Debug: Email {email} not found in authorized list")  # Debug info
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied: Your email address is not authorized to access this application. Please contact your administrator."
                )
            print(f"Debug: Email {email} is authorized")  # Debug info
        except HTTPException:
            # Re-throw access denied exception
            raise
        except Exception as e:
            print(f"Debug: Database authorization check failed: {str(e)}")  # Debug info
            # Database query failed, default deny access for security reasons
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
        
        print(f"Debug: Creating session for user: {email}")  # Debug info
        request.session["user"] = user_data
        
        access_token = AuthService.create_access_token(
            data={"user": user_data}
        )

        print("Debug: Authentication successful, redirecting to dashboard")  # Debug info
        
        # Redirect to dashboard after success instead of returning JSON
        return RedirectResponse(url="/dashboard", status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Debug: Unexpected error in callback: {str(e)}")  # Debug info
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
        # Read and validate the uploaded file
        if not emailJsonFile.filename.endswith('.json'):
            return templates.TemplateResponse("category.html", {
                "request": request,
                "error_message": "Only JSON files are allowed"
            })
        
        # Read file content
        content = await emailJsonFile.read()
        
        # Parse JSON content
        try:
            email_data = json.loads(content.decode('utf-8'))
        except json.JSONDecodeError:
            return templates.TemplateResponse("category.html", {
                "request": request,
                "error_message": "Invalid JSON format"
            })
        
        # Validate that it's a list of emails
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
        
        # Convert to EmailIn models for validation
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
        email_df = pd.DataFrame([e.model_dump(by_alias=True) for e in emails])
        cc = CategoryConnect(df=email_df)
        processed_df = cc.do_connect()
        
        # Convert to records for template display
        rows = processed_df.where(pd.notna(processed_df), None).to_dict(orient="records")
        
        # Clean up the data for template display
        processed_emails = []
        for row in rows:
            # Handle NaN values and ensure proper display format
            cleaned_row = {}
            for k, v in row.items():
                if pd.isna(v):
                    cleaned_row[k] = None
                elif isinstance(v, list):
                    cleaned_row[k] = v
                else:
                    cleaned_row[k] = str(v) if v is not None else None
            
            processed_emails.append(cleaned_row)
        
        # Return template with results
        return templates.TemplateResponse("category.html", {
            "request": request,
            "record_json": processed_emails,
            "filename": emailJsonFile.filename,
            "total_processed": len(processed_emails),
            "success_message": f"Successfully processed {len(processed_emails)} emails"
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
async def process_category_emails(
    request: Request,
    emailJsonFile: UploadFile = File(...),
    user=Depends(get_current_user)):
        result_dict = {}
    if request.method == 'POST':
      # test with file  
        # jsonFile = request.files.get('forwardEmailInJson')  
        # jsonList = json.load(jsonFile)
        # item = jsonList[0]
        # id = item['id']
        # correctedCategory = item['correctedCategory']
        # recipient = item['recipient']        
      # test with single ID  
        id = request.form.get('forwardEmailId') 
        if id:
            query = '''SELECT 
                            ecr."correctedCategory",
                            ecr."data" ->> 'recipient' AS "recipient"
                            FROM email_category_request ecr 
                            WHERE ecr."emailId"  = {EMAILID}''' 
            para = fetchFromDB(query.format(EMAILID=id))
            correctedCategory = para['correctedCategory'].iloc[0]
            recipient = para['recipient'].iloc[0]
            
        if correctedCategory in pp.forwCates:
            df = fetchFromDB(pp.emailSpecQuery.format(EMAILID=id)) 
            email = pp.main(df)
            
            email['correctedCategory'] = correctedCategory
            email['recipient'] = recipient
            email['textHtml'] = df['textHtml'].iloc[0]

            result = cf.main(email)
            result_dict = result.to_dict(orient='records')

        else:
            abort(400,description="Denna kategori kan inte vidarebefordras.")   
                
    return render_template('forward.html', record_json=result_dict)     
        
        
@app.post("/forward_api")

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
