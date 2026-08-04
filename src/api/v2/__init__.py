"""
API v2 Router package.
"""

from src.api.v2.claims_api import router as claims_router
from src.api.v2.search_api import router as search_router
from src.api.v2.analytics_api import router as analytics_router
from src.api.v2.admin_api import router as admin_router

__all__ = ["claims_router", "search_router", "analytics_router", "admin_router"]
