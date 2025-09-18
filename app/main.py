from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import os
from dotenv import load_dotenv

load_dotenv()

# Get project root directory path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

# Create FastAPI app
app = FastAPI(
    title="Email Processing API",
    description="FastAPI application for email categorization and forwarding",
    version="1.0.0"
)

# Add session middleware
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY", "your-secret-key"))

# Mount static files
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Setup templates
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Include API routers
from .api.auth import router as auth_router
from .api.category import router as category_router
from .api.forwarding import router as forwarding_router
from .api.payment import router as payment_router
from .api.summary import router as summary_router
from .api.log import router as log_router

app.include_router(auth_router, tags=["authentication"])
app.include_router(category_router, tags=["categorization"])
app.include_router(forwarding_router, tags=["forwarding"])
app.include_router(payment_router, tags=["payment"])
app.include_router(summary_router, tags=["summary"])
app.include_router(log_router, tags=["chronological-log"])

@app.get("/")
async def home():
    """Home page - redirect to login"""
    return RedirectResponse(url="/login", status_code=302)

@app.get("/dashboard")
async def dashboard(request: Request):
    """Dashboard page - requires authentication"""
    from .core.auth import security, get_current_user
    try:
        credentials = await security(request)
        user = get_current_user(request, credentials)
        user_display = user.get("name") or user.get("email") or "User" if isinstance(user, dict) else str(user)
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "user": user,
            "userId": user_display
        })
    except:
        return RedirectResponse(url="/login", status_code=302)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

# @app.get("/me")
# async def get_current_user_info(request: Request):
#     """Get current user authorization info"""
#     from .core.auth import security, get_optional_current_user, AuthService
#     import os
#     from datetime import timedelta
    
#     # Get credentials and user info
#     try:
#         credentials = await security(request)
#         user = get_optional_current_user(request, credentials)
#     except:
#         credentials = None
#         user = request.session.get("user")
    
#     # Generate test bearer token for Postman
#     test_token = None
#     if os.getenv("ENV_MODE") == "local" or not user:
#         # Create a test token for API testing
#         test_user_data = {
#             "sub": "test-user",
#             "email": "test@example.com", 
#             "name": "Test User",
#             "provider": "test"
#         }
#         test_token = AuthService.create_access_token(
#             data={"user": test_user_data},
#             expires_delta=timedelta(hours=24)
#         )
    
#     # Build authorization info
#     auth_info = {
#         "authenticated": user is not None,
#         "user": user,
#         "session_user": request.session.get("user"),
#         "environment": os.getenv("ENV_MODE", "production"),
#         "has_credentials": credentials is not None,
#         "credentials_scheme": credentials.scheme if credentials else None,
#         "session_keys": list(request.session.keys()) if hasattr(request, 'session') else [],
#         "headers": {
#             "authorization": request.headers.get("authorization"),
#             "x-api-key": request.headers.get("x-api-key"),
#             "user-agent": request.headers.get("user-agent")
#         },
#         "test_bearer_token": test_token,
#         "postman_instructions": {
#             "message": "Copy the test_bearer_token above and use it in Postman",
#             "steps": [
#                 "1. Copy the token from 'test_bearer_token' field above",
#                 "2. In Postman, go to Authorization tab",
#                 "3. Select 'Bearer Token' type",
#                 "4. Paste the token in the Token field",
#                 "5. Use this token for API testing"
#             ]
#         } if test_token else None
#     }
    
#     return auth_info

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)