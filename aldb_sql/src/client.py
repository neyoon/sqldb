from typing import Dict, List, Any, Optional, Union
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Boolean, Float, DateTime, JSON, 
    select, insert, update, delete,  
    create_engine
    )
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from schemas.micro_drama import models
from src.config import CONFIG


DATABASE_URL = CONFIG.get("SQL_DATABASE_URL", env_var="SQL_DATABASE_URL", default="sqlite+aiosqlite:///./test.db")

Base = declarative_base()
engine = create_engine(DATABASE_URL, echo=True, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

# 字符串到SQLAlchemy类型的映射
type_map = {
    "Integer": Integer,
    "String": String,
    "Boolean": Boolean,
    "Float": Float,
    "DateTime": DateTime,
    "JSON": JSON,
}

def build_column(col_def):
    col_type = type_map.get(col_def.type)
    if col_type is None:
        raise Exception(f"Unsupported column type: {col_def.type}")
    args = []
    kwargs = {
        "primary_key": col_def.primary_key,
        "nullable": col_def.nullable,
        "unique": col_def.unique,
        "autoincrement": col_def.autoincrement,
    }
    if col_def.default is not None:
        kwargs["default"] = col_def.default
    if col_def.type == "String" and col_def.length:
        args.append(col_def.length)
    return Column(col_def.name, col_type(*args), **kwargs)

class TableManager:
    """管理SQL表结构"""
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

    def create_table(self, name: str, columns: Optional[List[Any]] = None):
        if not columns or len(columns) == 0:
            raise Exception("columns must be provided and not empty")
        self.metadata.reflect(bind=self.engine)
        if name in self.metadata.tables:
            return False
        cols = [build_column(col) for col in columns]
        table = Table(name, self.metadata, *cols)
        table.create(self.engine)
        return True

    def delete_table(self, name: str):
        self.metadata.reflect(bind=self.engine)
        if name in self.metadata.tables:
            table = self.metadata.tables[name]
            table.drop(self.engine)
            return True
        return False

    def list_tables(self):
        self.metadata.reflect(bind=self.engine)
        return list(self.metadata.tables.keys())

class TableOperator:
    """对单表的CRUD操作"""
    def __init__(self, table: Table):
        self.table = table

    def insert(self, db, data: Union[Dict, List[Dict]], many: bool = False):
        try:
            # 提取所有表定义中的字段名
            valid_columns = [c.name for c in self.table.columns]

            # 清洗数据
            if many:
                cleaned_data = [
                    {k: v for k, v in row.items() if k in valid_columns}
                    for row in data  # ✅ 正确地遍历每个 dict
                ]
            else:
                cleaned_data = [{k: v for k, v in data.items() if k in valid_columns}]
            # 执行插入
            db.execute(insert(self.table), cleaned_data)
            db.commit()
            return True
        except SQLAlchemyError as e:
            db.rollback()
            raise Exception(f"Insert failed: {str(e)}")

    def find(self, db, query: Optional[Dict[str, Any]] = None, projection: Optional[List[str]] = None, sort: Optional[List] = None, skip: int = 0, limit: int = 0):
        if projection is not None and len(projection) > 0:
            stmt = select(*[getattr(self.table.c, col) for col in projection])
        else:
            stmt = select(self.table)
        if query is not None:
            for k, v in query.items():
                stmt = stmt.where(getattr(self.table.c, k) == v)
        if sort is not None:
            for col, direction in sort:
                stmt = stmt.order_by(getattr(self.table.c, col).asc() if direction == 'asc' else getattr(self.table.c, col).desc())
        if skip:
            stmt = stmt.offset(skip)
        if limit:
            stmt = stmt.limit(limit)
        result = db.execute(stmt)
        result_list = [dict(row._mapping) for row in result]
        return result_list

    def update(self, db, query: Dict, update_data: Dict):
        stmt = update(self.table)
        for k, v in query.items():
            stmt = stmt.where(getattr(self.table.c, k) == v)
        stmt = stmt.values(**update_data)
        result = db.execute(stmt)
        db.commit()
        return result.rowcount

    def delete(self, db, query: Dict):
        stmt = delete(self.table)
        for k, v in query.items():
            stmt = stmt.where(getattr(self.table.c, k) == v)
        result = db.execute(stmt)
        db.commit()
        return result.rowcount

class SQLDBInterface:
    def __init__(self):
        self.engine = engine
        self.metadata = metadata
        self.table_manager = TableManager(self.engine)
        self.session = SessionLocal

    def get_table_manager(self):
        return self.table_manager

    def get_table_operator(self, table_name: str):
        self.metadata.reflect(bind=self.engine)
        table = self.metadata.tables[table_name]
        return TableOperator(table) 