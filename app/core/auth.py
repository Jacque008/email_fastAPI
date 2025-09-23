from typing import Optional
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, status, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from authlib.integrations.starlette_client import OAuth
import jwt
import os
from ..services.utils import fetchFromDB

# Configuration
JWT_SECRET = os.getenv("JWT_SECRET") or "fallback-secret-key"
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 1  # Changed to 1 hour
SESSION_EXPIRATION_HOURS = 1  # Session also expires after 1 hour

security = HTTPBearer(auto_error=False)

# OAuth setup
oauth = OAuth()

# Ensure OAuth credentials are available
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET:
    oauth.register(
        name="google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
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
    def check_email_authorization(email: str, custom_query: Optional[str] = None) -> bool:
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
            if custom_query:
                query = custom_query
            else:
                query = "SELECT email FROM admin_user au"            
            whitelist_df = fetchFromDB(query)
            if whitelist_df.empty:
                return False
            authorized_emails = whitelist_df['email'].str.lower().tolist()
            return email.lower() in authorized_emails
            
        except Exception as e:
            # Log error and re-throw
            raise Exception(f"Database query failed: {str(e)}")

def _is_session_expired(login_time: str) -> bool:
    """Check if session timestamp has expired"""
    try:
        login_timestamp = datetime.fromisoformat(login_time)
        expiry_time = login_timestamp + timedelta(hours=SESSION_EXPIRATION_HOURS)
        return datetime.now(timezone.utc) > expiry_time
    except:
        return True  # Invalid timestamp = expired

def _get_test_user(request: Request) -> Optional[dict]:
    """Get test user for development mode"""
    if os.getenv("ENV_MODE") == "local":
        api_key = request.headers.get("X-API-Key")
        if api_key == os.getenv("TEST_API_KEY", "test-key-123"):
            return {
                "sub": "test-user",
                "email": "test@example.com",
                "name": "Test User",
                "provider": "api-key"
            }
    return None

def get_current_user(request: Request, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """Get current user (supports session and JWT with expiration)"""

    test_user = _get_test_user(request)
    if test_user:
        return test_user

    if credentials:
        payload = AuthService.verify_token(credentials.credentials)
        user = payload.get("user")
        if user:
            return user

    user = request.session.get("user")
    login_time = request.session.get("login_time")

    if user and login_time:
        if _is_session_expired(login_time):
            request.session.clear()
            user = None

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user

def get_optional_current_user(request: Request, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """Get current user (optional, no exception thrown)"""
    try:
        return get_current_user(request, credentials)
    except HTTPException:
        return None