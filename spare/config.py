# Configuration File - Email Authorization Settings
"""
Email Authorization Configuration File

This file contains configuration options for email authorization validation. 
You can modify these settings according to your actual requirements.
"""

# Default email authorization query
DEFAULT_EMAIL_AUTH_QUERY = """
SELECT email 
FROM authorized_users 
WHERE active = true
"""

# Example custom queries (you can modify according to your actual table structure)
CUSTOM_QUERIES = {
    # Filter by department
    "IT_DEPARTMENT": """
        SELECT email 
        FROM employees 
        WHERE department = 'IT' 
        AND status = 'active' 
        AND access_level >= 3
    """,
    
    # Filter by role
    "ADMIN_USERS": """
        SELECT email 
        FROM users u
        JOIN user_roles ur ON u.id = ur.user_id
        JOIN roles r ON ur.role_id = r.id
        WHERE r.name IN ('admin', 'super_admin')
        AND u.active = true
    """,
    
    # Filter by project access permissions
    "PROJECT_ACCESS": """
        SELECT u.email
        FROM users u
        JOIN project_permissions pp ON u.id = pp.user_id
        JOIN projects p ON pp.project_id = p.id
        WHERE p.name = 'DRP_EMAIL_PROCESSING'
        AND pp.permission_level >= 'read'
        AND u.active = true
    """,
    
    # Simple email whitelist table
    "SIMPLE_WHITELIST": """
        SELECT email_address as email
        FROM email_whitelist
        WHERE is_active = 1
    """
}

# Authorization configuration
AUTH_CONFIG = {
    # Whether to enable email authorization check
    "ENABLE_EMAIL_AUTHORIZATION": True,
    
    # Default query type to use (can be 'DEFAULT' or any key from CUSTOM_QUERIES)
    "DEFAULT_QUERY_TYPE": "DEFAULT",
    
    # Whether to allow authentication when database query fails (for security, recommend False)
    "ALLOW_AUTH_ON_DB_FAILURE": False,
    
    # Error message when authorization fails
    "AUTH_FAILURE_MESSAGE": "Access denied: Your email address is not authorized for this application. Please contact your administrator.",
    
    # Error message when database query fails
    "DB_FAILURE_MESSAGE": "Authorization service temporarily unavailable. Please try again later."
}

def get_email_auth_query(query_type: str = None) -> str:
    """
    Get email authorization query statement
    
    Args:
        query_type: Query type, can be 'DEFAULT' or any key from CUSTOM_QUERIES
    
    Returns:
        str: SQL query statement
    """
    query_type = query_type or AUTH_CONFIG["DEFAULT_QUERY_TYPE"]
    
    if query_type == "DEFAULT":
        return DEFAULT_EMAIL_AUTH_QUERY
    elif query_type in CUSTOM_QUERIES:
        return CUSTOM_QUERIES[query_type]
    else:
        raise ValueError(f"Unknown query type: {query_type}")
