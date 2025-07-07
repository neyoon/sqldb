from fastapi import APIRouter
from api.routes import collection_data, collections

api_router = APIRouter()

api_router.include_router(collection_data.router, prefix="/api/v1/data", tags=["data"])
api_router.include_router(collections.router, prefix="/api/v1/table", tags=["table"]) 