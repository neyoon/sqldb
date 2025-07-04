from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class ColumnDef(BaseModel):
    name: str
    type: str  # 如 'Integer', 'String', 'Boolean', 'Float', 'DateTime'
    primary_key: Optional[bool] = False
    nullable: Optional[bool] = True
    unique: Optional[bool] = False
    autoincrement: Optional[bool] = False
    default: Optional[Any] = None
    length: Optional[int] = None  # 针对String类型

class CollectionCreate(BaseModel):
    name: str
    columns: List[ColumnDef]

class QueryParams(BaseModel):
    query: Optional[Dict[str, Any]] = Field(default_factory=dict)
    projection: Optional[List[str]] = None  # SQL只支持字段名列表
    sort: Optional[List[tuple[str, str]]] = None  # [(字段, 'asc'/'desc')]
    skip: Optional[int] = 0
    limit: Optional[int] = 0

class UpdateData(BaseModel):
    query: Dict[str, Any]
    update_data: Dict[str, Any]
    many: Optional[bool] = False
    upsert: Optional[bool] = False

class DeleteQuery(BaseModel):
    query: Dict[str, Any]
    many: Optional[bool] = False

class DataResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    message: Optional[str] = None 