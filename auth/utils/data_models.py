from enum import Enum
from typing import List
from typing import Optional

from pydantic import BaseModel

from data_models import SnapshotterMetadata


class UserStatusEnum(str, Enum):
    """Enum representing the status of a user."""
    active = 'active'
    inactive = 'inactive'


class AddApiKeyRequest(BaseModel):
    """Model for adding an API key request."""
    api_key: str


class UserAllDetailsResponse(SnapshotterMetadata):
    """Model for user details response, including API keys."""
    active_api_keys: List[str]
    revoked_api_keys: List[str]


class AuthCheck(BaseModel):
    """Model for authentication check results."""
    authorized: bool = False  # Whether the authentication was successful
    api_key: str  # The API key used for authentication
    reason: str = ''  # Reason for authentication failure, if any
    # Owner details if authenticated
    owner: Optional[SnapshotterMetadata] = None


class RateLimitAuthCheck(AuthCheck):
    """Model for rate-limited authentication check results."""
    rate_limit_passed: bool = False  # Whether the rate limit check passed
    retry_after: int = 1  # Seconds to wait before retrying if rate limited
    violated_limit: str  # Description of the violated rate limit
    current_limit: str  # Current rate limit in effect
