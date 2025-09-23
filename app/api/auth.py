from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from authlib.integrations.starlette_client import OAuthError
import os
from ..core.auth import oauth, AuthService


# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/login")
async def login_page(request: Request, force_fresh: bool = False):
    """OAuth login page - check authentication or force fresh login"""

    if force_fresh:
        request.session.clear()
        return RedirectResponse(url="/auth/google", status_code=302)

    try:
        from ..core.auth import get_current_user
        from ..core.auth import security
        credentials = await security(request)
        if credentials:
            user = get_current_user(request, credentials)
        else:
            raise HTTPException(status_code=401, detail="No credentials")

        user_display = user.get("name") or user.get("email") or "User"
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "user": user,
            "userId": user_display
        })
    except:
        request.session.clear()
        return RedirectResponse(url="/auth/google", status_code=302)

@router.get("/auth/google")
async def google_auth(request: Request):
    """Initiate Google OAuth login with forced account selection"""
    if not oauth.google:
        raise HTTPException(status_code=500, detail="Google OAuth is not configured")

    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI") or request.url_for("google_callback")
    state = os.urandom(32).hex()

    # Set OAuth state
    request.session.clear()
    request.session["oauth_state"] = state

    # Force account selection and consent to prevent auto-login
    return await oauth.google.authorize_redirect(
        request, redirect_uri, state=state, prompt="select_account consent"
    )

@router.get("/auth/google/callback")
async def google_callback(request: Request):
    """Handle Google OAuth callback"""
    if not oauth.google:
        raise HTTPException(status_code=500, detail="Google OAuth is not configured")

    try:
        # Verify state parameter for CSRF protection
        state = request.query_params.get("state")
        session_state = request.session.get("oauth_state")

        if not state or not session_state or state != session_state:
            request.session.clear()
            raise HTTPException(status_code=400, detail="Invalid OAuth state. Please try again.")
        
        # Get user info from Google
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get("userinfo")

        if not user_info or not user_info.get("email"):
            raise HTTPException(status_code=400, detail="Failed to get user info from Google")

        # Check email authorization
        user_email = user_info.get("email")
        if not AuthService.check_email_authorization(user_email):
            raise HTTPException(status_code=403, detail=f"Access denied for email: {user_email}")

        # Store user session with timestamp
        from datetime import datetime, timezone
        request.session["user"] = {
            "sub": user_info.get("sub"),
            "email": user_email,
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "provider": "google"
        }
        request.session["login_time"] = datetime.now(timezone.utc).isoformat()
        request.session.pop("oauth_state", None)

        return RedirectResponse(url="/dashboard", status_code=302)
        
    except OAuthError as e:
        raise HTTPException(status_code=400, detail=f"OAuth error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")


@router.get("/logout")
async def logout_get(request: Request):
    """Logout user (GET method for browser access) with complete credential invalidation"""
    # Clear session completely
    request.session.clear()

    # Use template response with JavaScript cleanup
    response = templates.TemplateResponse("logout.html", {
        "request": request,
        "message": "You have been logged out successfully! All credentials have been cleared."
    })

    # Server-side cookie clearing
    response.delete_cookie("session", path="/")
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")
    response.delete_cookie("jwt", path="/")
    response.delete_cookie("session", path="/", domain="127.0.0.1")
    response.delete_cookie("session", path="/", domain="localhost")

    # Cache control headers
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, private"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response