from typing import Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Depends
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from authlib.integrations.starlette_client import OAuthError
import os
from ..core.auth import oauth, AuthService
from .deps import get_optional_user

# Get templates directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

@router.get("/login")
async def login_page(request: Request, user=Depends(get_optional_user)):
    """OAuth login page - shows dashboard if logged in, otherwise redirects to Google OAuth"""
    if user:
        # Extract user name/email for display
        user_display = user.get("name") or user.get("email") or "User" if isinstance(user, dict) else str(user)
        return templates.TemplateResponse("dashboard.html", {
            "request": request, 
            "user": user,
            "userId": user_display
        })
    
    # Redirect directly to Google OAuth instead of showing a message
    return RedirectResponse(url="/auth/google", status_code=status.HTTP_302_FOUND)

@router.get("/auth/google")
async def google_auth(request: Request):
    """Initiate Google OAuth login"""
    if not oauth.google:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth is not configured"
        )
    
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI") or request.url_for("google_callback")
    state = os.urandom(32).hex()
    
    # Clear any existing state and set new one
    request.session.clear()
    request.session["oauth_state"] = state
    
    print(f"DEBUG Auth - Generated state: {state}")
    print(f"DEBUG Auth - Stored in session: {request.session.get('oauth_state')}")
    print(f"DEBUG Auth - Using redirect_uri: {redirect_uri}")
    
    return await oauth.google.authorize_redirect(request, redirect_uri, state=state)

@router.get("/auth/google/callback")
async def google_callback(request: Request):
    """Handle Google OAuth callback"""
    if not oauth.google:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth is not configured"
        )
    
    try:
        # Verify state parameter
        state = request.query_params.get("state")
        session_state = request.session.get("oauth_state")
        
        print(f"DEBUG Callback - URL state: {state}")
        print(f"DEBUG Callback - Session state: {session_state}")
        print(f"DEBUG Callback - Session data: {dict(request.session)}")
        
        if not state:
            print(f"DEBUG: No state in URL")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing state parameter in callback URL"
            )
        
        if not session_state:
            print(f"DEBUG: No state in session - session may have expired")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Session expired. Please try logging in again."
            )
            
        if state != session_state:
            print(f"DEBUG: State mismatch - URL: '{state}', Session: '{session_state}'")
            # Clear the bad session state
            request.session.clear()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Session state mismatch. Please try logging in again."
            )
        
        # Get access token
        token = await oauth.google.authorize_access_token(request)
        user_info = token.get("userinfo")
        
        if not user_info:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get user info from Google"
            )
        
        # Check if user is authorized
        user_email = user_info.get("email")
        if not user_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No email found in user info"
            )
        
        # Verify email authorization
        try:
            is_authorized = AuthService.check_email_authorization(user_email)
            if not is_authorized:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Access denied for email: {user_email}"
                )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Authorization check failed: {str(e)}"
            )
        
        # Store user info in session
        user_data = {
            "sub": user_info.get("sub"),
            "email": user_email,
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "provider": "google"
        }
        request.session["user"] = user_data
        
        # Clean up OAuth state
        request.session.pop("oauth_state", None)
        
        # Redirect to dashboard after successful login
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
        
    except OAuthError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"OAuth error: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Authentication failed: {str(e)}"
        )

@router.post("/auth/token")
async def create_token(request: Request, user=Depends(get_optional_user)) -> Dict[str, Any]:
    """Create JWT token for authenticated user"""
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    # Create JWT token
    access_token = AuthService.create_access_token(data={"user": user})
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user
    }

@router.get("/auth/me")
async def get_current_user_info(user=Depends(get_optional_user)) -> Dict[str, Any]:
    """Get current user information"""
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    return {"user": user}

@router.post("/auth/logout")
async def logout(request: Request):
    """Logout user"""
    request.session.clear()
    return {"message": "Logged out successfully"}

@router.post("/logout")
async def logout_root(request: Request):
    """Logout user (root path)"""
    request.session.clear()
    return {"message": "Logged out successfully"}

@router.get("/logout")
async def logout_get(request: Request):
    """Logout user (GET method for browser access)"""
    request.session.clear()
    return {"message": "Logged out successfully"}