from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from schemas.micro_drama.models import (
    CollectionCreate,
    DataResponse
)
from src.client import SQLDBInterface

router = APIRouter()
db = SQLDBInterface()

@router.post("/create", response_model=DataResponse)
async def create_table(collection_data: CollectionCreate):
    try:
        table_manager = db.get_table_manager()
        # columns为Pydantic对象列表，需转为dict
        columns = [col for col in collection_data.columns]
        success = table_manager.create_table(collection_data.name, columns)
        return DataResponse(
            success=success,
            message="Table created successfully" if success else "Table already exists"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/list", response_model=DataResponse)
async def list_tables():
    try:
        table_manager = db.get_table_manager()
        tables = table_manager.list_tables()
        return DataResponse(success=True, data=tables)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/delete/{table_name}", response_model=DataResponse)
async def delete_table(table_name: str):
    try:
        table_manager = db.get_table_manager()
        success = table_manager.delete_table(table_name)
        tables = table_manager.list_tables()
        return DataResponse(
            success=success,
            message=f"Table deleted successfully, here are the tables now:{tables}" if success else "Table not found"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 