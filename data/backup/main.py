from fastapi import FastAPI, Request, HTTPException
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

# Exception handler for JWT expiration
@app.exception_handler(HTTPException)
async def auth_exception_handler(request: Request, exc: HTTPException):
    """Handle authentication exceptions by redirecting to login"""
    if exc.status_code == 401 and ("Token has expired" in exc.detail or "Not authenticated" in exc.detail):
        # Clear session for expired tokens
        request.session.clear()
        return RedirectResponse(url="/login", status_code=302)
    # Re-raise other HTTP exceptions
    raise exc

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
from .api.update_clinic import router as clinic_router

app.include_router(auth_router, tags=["authentication"])
app.include_router(category_router, tags=["categorization"])
app.include_router(forwarding_router, tags=["forwarding"])
app.include_router(payment_router, tags=["payment"])
app.include_router(summary_router, tags=["summary"])
app.include_router(log_router, tags=["chronological-log"])
app.include_router(clinic_router, tags=["clinic-update"])

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
    except Exception:
        request.session.clear()
        return RedirectResponse(url="/login", status_code=302)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)