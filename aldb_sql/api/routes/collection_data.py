from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from schemas.micro_drama.models import (
    QueryParams,
    UpdateData,
    DeleteQuery,
    DataResponse
)
from src.client import SQLDBInterface

router = APIRouter()
db = SQLDBInterface()

# Dependency
def get_db():
    db_session = db.session()
    try:
        yield db_session
    finally:
        db_session.close()

@router.post("/{table_name}/documents", response_model=DataResponse)
async def insert_document(table_name: str, document: Dict[str, Any], db_session: Session = Depends(get_db)):
    try:
        operator = db.get_table_operator(table_name)
        operator.insert(db_session, document)
        return DataResponse(success=True, data={"inserted": True})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{table_name}/documents/bulk", response_model=DataResponse)
async def insert_many_documents(table_name: str, documents: List[Dict[str, Any]], db_session: Session = Depends(get_db)):
    try:
        operator = db.get_table_operator(table_name)
        operator.insert(db_session, documents, many=True)
        return DataResponse(success=True, data={"inserted": len(documents)})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{table_name}/query", response_model=DataResponse)
async def query_documents(table_name: str, params: QueryParams, db_session: Session = Depends(get_db)):
    try:
        operator = db.get_table_operator(table_name)
        results = operator.find(
            db_session,
            query=params.query,
            projection=params.projection,
            sort=params.sort,
            skip=params.skip,
            limit=params.limit
        )
        return DataResponse(success=True, data=results)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/{table_name}/documents", response_model=DataResponse)
async def update_documents(table_name: str, update_data: UpdateData, db_session: Session = Depends(get_db)):
    try:
        operator = db.get_table_operator(table_name)
        modified_count = operator.update(
            db_session,
            query=update_data.query,
            update_data=update_data.update_data,
            many=update_data.many,
            upsert=update_data.upsert
        )
        return DataResponse(success=True, data={"modified_count": modified_count})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{table_name}/documents", response_model=DataResponse)
async def delete_documents(table_name: str, delete_query: DeleteQuery, db_session: Session = Depends(get_db)):
    try:
        operator = db.get_table_operator(table_name)
        deleted_count = operator.delete(
            db_session,
            query=delete_query.query,
            many=delete_query.many
        )
        return DataResponse(success=True, data={"deleted_count": deleted_count})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 