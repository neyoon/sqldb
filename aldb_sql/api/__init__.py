from fastapi import APIRouter
from api.routes import collection_data, collections

api_router = APIRouter()

api_router.include_router(collection_data.router, prefix="/data", tags=["data"])
api_router.include_router(collections.router, prefix="/table", tags=["table"]) 