from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials
from ..core.auth import get_current_user, get_optional_current_user, security

# Re-export common dependencies for easy import
def get_current_active_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current authenticated user"""
    return get_current_user(request, credentials)

def get_optional_user(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get current user (optional)"""
    return get_optional_current_user(request, credentials)