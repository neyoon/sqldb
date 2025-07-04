from typing import Dict, List, Any, Optional, Union
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Float, DateTime, select, insert, update, delete, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

DATABASE_URL = "sqlite+aiosqlite:///./test.db"

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
        self.metadata = MetaData(bind=engine)

    def create_table(self, name: str, columns: Optional[List[Any]] = None):
        if not columns or len(columns) == 0:
            raise Exception("columns must be provided and not empty")
        if name in self.metadata.tables:
            return False
        cols = [build_column(col) for col in columns]
        table = Table(name, self.metadata, *cols)
        table.create(self.engine)
        return True

    def delete_table(self, name: str):
        if name in self.metadata.tables:
            table = self.metadata.tables[name]
            table.drop(self.engine)
            return True
        return False

    def list_tables(self):
        self.metadata.reflect()
        return list(self.metadata.tables.keys())

class TableOperator:
    """对单表的CRUD操作"""
    def __init__(self, table: Table):
        self.table = table

    def insert(self, db, data: Union[Dict, List[Dict]], many: bool = False):
        try:
            if many:
                db.execute(insert(self.table), data)
            else:
                db.execute(insert(self.table), [data])
            db.commit()
            return True
        except SQLAlchemyError as e:
            db.rollback()
            raise Exception(f"Insert failed: {str(e)}")

    def find(self, db, query: Dict = None, projection: List[str] = None, sort: List = None, skip: int = 0, limit: int = 0):
        stmt = select(self.table)
        if query:
            for k, v in query.items():
                stmt = stmt.where(getattr(self.table.c, k) == v)
        if projection:
            stmt = select(*[getattr(self.table.c, col) for col in projection])
        if sort:
            for col, direction in sort:
                stmt = stmt.order_by(getattr(self.table.c, col).asc() if direction == 'asc' else getattr(self.table.c, col).desc())
        if skip:
            stmt = stmt.offset(skip)
        if limit:
            stmt = stmt.limit(limit)
        result = db.execute(stmt)
        return [dict(row) for row in result]

    def update(self, db, query: Dict, update_data: Dict, many: bool = False, upsert: bool = False):
        stmt = update(self.table)
        for k, v in query.items():
            stmt = stmt.where(getattr(self.table.c, k) == v)
        stmt = stmt.values(**update_data)
        result = db.execute(stmt)
        db.commit()
        return result.rowcount

    def delete(self, db, query: Dict, many: bool = False):
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