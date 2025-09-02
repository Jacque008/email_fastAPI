from fastapi import FastAPI, Request
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

app.include_router(auth_router, tags=["authentication"])
app.include_router(category_router, tags=["categorization"])
app.include_router(forwarding_router, tags=["forwarding"])

# Root endpoint
@app.get("/")
async def home(request: Request):
    """Home page"""
    return templates.TemplateResponse("index.html", {"request": request})

# Dashboard endpoint
@app.get("/dashboard")
async def dashboard(request: Request):
    """Dashboard page"""
    from .api.deps import get_optional_user
    from fastapi import Depends
    from fastapi.responses import RedirectResponse
    
    # Try to get current user
    user = None
    try:
        from .core.auth import security, get_optional_current_user
        credentials = security.__call__(request)
        user = get_optional_current_user(request, credentials) if credentials else None
    except:
        # Check session for user
        user = request.session.get("user")
    
    if user:
        # Extract user name/email for display
        user_display = user.get("name") or user.get("email") or "User" if isinstance(user, dict) else str(user)
        return templates.TemplateResponse("dashboard.html", {
            "request": request, 
            "user": user,
            "userId": user_display
        })
    else:
        # Redirect to login if not authenticated
        return RedirectResponse(url="/login", status_code=302)

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)