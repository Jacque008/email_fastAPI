from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv

load_dotenv()

# Get project root directory path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        print(f"🚀 FastAPI app starting up...")
        print(f"   ENV_MODE: {os.getenv('ENV_MODE', 'not_set')}")
        print(f"   PORT: {os.getenv('PORT', 'not_set')}")
        print(f"   SECRET_KEY_FASTAPI: {'set' if os.getenv('SECRET_KEY_FASTAPI') else 'not_set'}")
        print(f"✅ Startup completed successfully")
    except Exception as e:
        print(f"❌ Error during startup: {e}")
        # Don't raise exception to prevent startup failure

    yield

    # Shutdown
    print("👋 FastAPI app shutting down...")

# Create FastAPI app
app = FastAPI(
    title="Email Processing API",
    description="FastAPI application for email categorization and forwarding",
    version="1.0.0",
    lifespan=lifespan
)

# Add session middleware
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY_FASTAPI", "your-secret-key"))

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

# Add global exception handler to prevent crashes
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all unhandled exceptions to prevent crashes"""
    import traceback

    # Log the error
    print(f"🚨 Unhandled exception: {type(exc).__name__}: {str(exc)}")
    print(f"Request: {request.method} {request.url}")
    print(f"Traceback: {traceback.format_exc()[-500:]}")

    # Return a safe JSON response
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "type": type(exc).__name__,
            "detail": str(exc)[:200]
        }
    )

@app.get("/")
async def home():
    """Home page - redirect to login"""
    return RedirectResponse(url="/login", status_code=302)

@app.get("/dashboard")
async def dashboard(request: Request):
    """Dashboard page - requires authentication"""
    import os

    # Bypass auth for testing if OAuth not configured
    if not os.getenv("GOOGLE_CLIENT_ID"):
        return {
            "status": "dashboard_test_mode",
            "message": "Dashboard accessible (OAuth not configured)",
            "user": "test_user",
            "templates_dir": TEMPLATES_DIR,
            "templates_exist": os.path.exists(TEMPLATES_DIR)
        }

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
    try:
        import psutil
        import os

        # Get basic system info without triggering service initialization
        process = psutil.Process()
        memory_info = process.memory_info()

        return {
            "status": "healthy",
            "env_mode": os.getenv("ENV_MODE", "not_set"),
            "memory_mb": round(memory_info.rss / 1024 / 1024, 2),
            "pid": os.getpid(),
            "timestamp": str(os.times())
        }
    except Exception as e:
        print(f"Health check error: {e}")
        # Ensure we always return valid JSON
        return {"status": "error", "error": str(e)[:200]}

@app.get("/test-simple")
async def test_simple():
    """Simple test that should always work"""
    try:
        import time
        return {
            "status": "simple_ok",
            "timestamp": time.time(),
            "message": "This is a simple test",
            "data": {"key1": "value1", "key2": "value2"}
        }
    except Exception as e:
        return {"status": "simple_error", "error": str(e)}

@app.get("/test-minimal")
async def test_minimal():
    """Most minimal test possible"""
    print("🧪 Starting minimal test")
    try:
        result = {"status": "minimal_ok", "message": "This is minimal"}
        print("🧪 Minimal test returning result")
        return result
    except Exception as e:
        print(f"🧪 Minimal test error: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/test-import")
async def test_import():
    """Test just the import"""
    try:
        print("Testing import...")
        from .services.services import DefaultServices
        print("Import successful")
        return {"status": "import_ok", "class": str(DefaultServices)}
    except Exception as e:
        print(f"Import failed: {e}")
        return {"status": "import_error", "error": str(e)[:200]}

@app.get("/test-baseservice")
async def test_baseservice():
    """Test BaseService creation directly"""
    try:
        print("🔍 Testing BaseService creation...")
        from .services.base_service import BaseService

        print("🏗️ Creating BaseService instance...")
        base_service = BaseService()
        print("✅ BaseService created successfully")

        print("🔢 Accessing attributes...")
        folder = base_service.folder
        fb_count = len(base_service.fb_ref_list)
        print(f"✅ Attributes accessed: folder={folder}, fb_count={fb_count}")

        print("🎯 Returning response...")
        response = {
            "status": "baseservice_ok",
            "folder": folder,
            "fb_ref_count": fb_count,
            "clinic_rows": len(base_service.clinic)
        }
        print(f"✅ Response ready: {response}")
        return response

    except Exception as e:
        import traceback
        error_msg = str(e)[:200]
        trace = traceback.format_exc()[-300:]
        print(f"❌ BaseService error: {error_msg}")
        print(f"❌ Traceback: {trace}")
        return {
            "status": "baseservice_error",
            "error": error_msg,
            "traceback": trace
        }

@app.get("/test-lazy-services")
async def test_lazy_services():
    """Test if services work without immediate BaseService creation"""
    try:
        print("🧪 Testing lazy services...")
        from .services.services import DefaultServices

        print("🧪 Creating DefaultServices container...")
        services = DefaultServices()
        print("🧪 DefaultServices container created")

        # Test lazy loading - don't actually create BaseService instances yet
        print("🧪 Testing service availability...")
        result = {
            "status": "lazy_services_ok",
            "message": "DefaultServices created without BaseService initialization",
            "has_processor": hasattr(services, '_processor'),
            "has_parser": hasattr(services, '_parser')
        }
        print(f"🧪 Lazy services result: {result}")
        return result

    except Exception as e:
        import traceback
        error_msg = str(e)[:200]
        trace = traceback.format_exc()[-500:]
        print(f"❌ Lazy services error: {error_msg}")
        return {
            "status": "lazy_services_error",
            "error": error_msg,
            "traceback": trace
        }

@app.get("/test-processor-only")
async def test_processor_only():
    """Test creating just the processor service"""
    try:
        print("🧪 Testing processor creation...")
        from .services.services import DefaultServices

        services = DefaultServices()
        print("🧪 Getting processor...")

        # This will trigger BaseService creation
        processor = services.get_processor()
        print("🧪 Processor obtained")

        return {
            "status": "processor_ok",
            "processor_type": str(type(processor)),
            "processor_exists": processor is not None
        }

    except Exception as e:
        import traceback
        error_msg = str(e)[:200]
        trace = traceback.format_exc()[-300:]
        print(f"❌ Processor test error: {error_msg}")
        return {
            "status": "processor_error",
            "error": error_msg,
            "traceback": trace
        }

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
    uvicorn.run(app, host="0.0.0.0", port=5000)