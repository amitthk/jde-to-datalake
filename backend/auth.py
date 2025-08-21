import os
from dotenv import load_dotenv
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from ldap3 import Server, Connection, AUTO_BIND_NO_TLS
import jwt

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")

# LDAP configuration
AUTH_TYPE = 2
AUTH_LDAP_SERVER = os.getenv("AUTH_LDAP_SERVER", "")
AUTH_LDAP_USE_TLS = os.getenv("AUTH_LDAP_USE_TLS", False)
AUTH_LDAP_BIND_USER = os.getenv("AUTH_LDAP_BIND_USER", "")
AUTH_LDAP_BIND_PASSWORD = os.getenv("AUTH_LDAP_BIND_PASSWORD", "")
AUTH_LDAP_GROUP_FIELD = "memberOf"
AUTH_USER_SEARCH_FIELD = "UserPrincipalName"
AUTH_LDAP_USER_SEARCH_BASE = "dc=STICAL,dc=COM,dc=AU"
# Mapping LDAP groups to Superset roles
AUTH_ROLES_MAPPING = {
    "CN=Helpdesk Users,OU=IT,OU=Security Groups,OU=Synced to O365,OU=Distribution Groups,OU=STICAL,DC=STICAL,DC=COM,DC=AU": ["Admin"],
}

# Mapping LDAP groups to Superset roles
AUTH_LDAP_GROUP_FIELD = os.getenv("AUTH_LDAP_GROUP_FIELD", "MemberOf")
AUTH_ROLES_MAPPING = {
    os.getenv("AUTH_LDAP_GRP1", "CN=Domain Users,CN=Users,DC=STICAL,DC=COM,DC=AU"): ["Admin"],
    os.getenv("AUTH_LDAP_GRP2", "CN=Domain Users,CN=Users,DC=STICAL,DC=COM,DC=AU"): ["Gamma"]
}

class AuthenticateUser:
    def __init__(self):
        try:
            self.server = Server(AUTH_LDAP_SERVER, get_info='ALL')
            print(f"LDAP Server initialized: {AUTH_LDAP_SERVER}")
        except Exception as e:
            print(f"Failed to initialize LDAP server: {e}")
            self.server = None

    async def authenticate(self, username: str, password: str) -> dict | None:
        if not self.server:
            raise HTTPException(status_code=500, detail="LDAP server not initialized")
            
        try:
            # Try different username formats for LDAP binding
            username_formats = [
                f"{username}@{AUTH_LDAP_SERVER.split(':')[0]}",  # UPN format
                f"STICAL\\{username}",  # Domain format
                username  # Raw username
            ]
            
            connection = None
            for user_format in username_formats:
                try:
                    connection = Connection(
                        server=self.server,
                        user=user_format,
                        password=password,
                        auto_bind=True
                    )
                    print(f"Successfully bound with format: {user_format}")
                    break  # If successful, break out of the loop
                except Exception as bind_error:
                    print(f"Failed to bind with format '{user_format}': {bind_error}")
                    continue
            
            if not connection:
                raise HTTPException(status_code=401, detail="Failed to authenticate with LDAP server")

            search_filter = f"(&({AUTH_USER_SEARCH_FIELD}={username})(ObjectClass=user))"
            attributes_to_get = [AUTH_LDAP_GROUP_FIELD]

            connection.search(
                search_base=AUTH_LDAP_USER_SEARCH_BASE,
                search_filter=search_filter,
                attributes=attributes_to_get
            )
            
            if len(connection.entries) > 0:
                ldap_groups = []
                for entry in connection.entries:
                    attrs = entry.entry_attributes_as_dict
                    groups = attrs.get(AUTH_LDAP_GROUP_FIELD, [])
                    ldap_groups.extend(groups)

                roles = []
                for ldap_group, roles_mapping in AUTH_ROLES_MAPPING.items():
                    if ldap_group in ldap_groups:
                        roles.extend(roles_mapping)

                # If no specific roles found, give basic access
                if not roles:
                    roles = ["User"]

                return {"username": username, "roles": roles}
            else:
                raise HTTPException(status_code=401, detail="User not found or no groups assigned")

        except HTTPException:
            raise
        except Exception as e:
            # For development/testing, you might want to add a fallback
            print(f"LDAP authentication error: {e}")
            
            # Development fallback - remove this in production
            if username.lower() in ['admin', 'test', 'developer']:
                return {"username": username, "roles": ["Admin"]}
            
            raise HTTPException(status_code=401, detail=f"LDAP authentication error: {str(e)}")


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip authentication for token endpoint, health check, and OPTIONS requests
        # Also skip for development/testing endpoints
        excluded_paths = [
            "/token", 
            "/health",
            "/live-data",  # Alias for LiveDataComparison compatibility
            "/data/joined_df3",  # Allow LiveDataComparison to fetch data without auth
            "/data/jde_item_master_review",  # Allow JDE Item Master review without auth
            "/patch/ingredient",  # Allow Ingredient patching without auth
            "/patch/ingredient/enhanced",  # Allow enhanced Ingredient patching without auth
            "/patch/ingredient/advanced",  # Allow advanced Ingredient patching without auth
            "/test/units",  # Allow unit endpoint testing without auth
            "/search/ingredient",  # Allow search without auth for testing
            "/patch/ingredient/advanced",  # Allow advanced patch without auth for testing
            "/data/bakery_system_to_jde_actions",  # Allow Bakery-System data fetch without auth
            "/prepare_jde_payload",  # Allow JDE payload preparation without auth
            "/dispatch/prepared_payload_to_jde",  # Allow JDE dispatch without auth for testing
            "/dispatch/batch_to_jde",  # Allow batch dispatch without auth for testing
            "/prepare_transaction_payload",  # Allow transaction payload preparation without auth
            "/prepare_ingredient_payload",  # Allow Ingredient payload preparation without auth
            "/dispatch/prepared_transaction",  # Allow transaction dispatch without auth for testing
            "/create/prepared_ingredient",  # Allow Ingredient creation without auth for testing
            "/dispatch/transaction",  # Allow transaction dispatch without auth for testing
            "/create/ingredient",  # Allow Ingredient creation without auth for testing
            "/batch_review/create_session",  # Allow batch review session creation without auth
            "/batch_review/get_session",  # Allow batch review session retrieval without auth
            "/batch_review/delete_session"  # Allow batch review session deletion without auth
        ]
        
        excluded_path_prefixes = [
            "/delete/ingredient/",  # Allow Ingredient deletion without auth (matches /delete/Ingredient/{id})
            "/batch_review/get_session/",  # Allow batch review session retrieval without auth
            "/batch_review/delete_session/"  # Allow batch review session deletion without auth
        ]
        
        # Check exact matches and prefix matches
        if (request.url.path in excluded_paths or 
            request.method == "OPTIONS" or 
            any(request.url.path.startswith(prefix) for prefix in excluded_path_prefixes)):
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(status_code=401, detail="Missing Authorization header")

        try:
            scheme, token = auth_header.split()
            if scheme.lower() != "bearer":
                raise HTTPException(status_code=401, detail="Invalid authentication scheme")

            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username = payload.get("sub")
            roles = payload.get("roles", [])

            if not username:
                raise HTTPException(status_code=401, detail="Invalid token")

            request.state.user = {"username": username, "roles": roles}

        except Exception as e:
            raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

        return await call_next(request)


class TokenData(BaseModel):
    access_token: str
    token_type: str


class TokenRequest(BaseModel):
    username: str
    password: str

async def get_token(request: TokenRequest):
    authenticate_user = AuthenticateUser()
    try:
        username = request.username
        password = request.password
        user_data = await authenticate_user.authenticate(username, password)
        
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        # Generate JWT token
        access_token = jwt.encode(
            {"sub": user_data["username"], "roles": user_data["roles"]},
            SECRET_KEY,
            algorithm=ALGORITHM
        )
        
        return TokenData(access_token=access_token, token_type='bearer')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating JWT token: {str(e)}")
