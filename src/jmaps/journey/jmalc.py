import numpy as np
from datetime import datetime
from sqlalchemy import (TIMESTAMP, and_, or_, create_engine, event, 
    case, cast, select, Column, ForeignKey, ForeignKeyConstraint, literal_column,
    Integer, Float, DateTime, null,
    String, Unicode, UnicodeText)
from sqlalchemy import Computed
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, Session, object_session
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy.orm.interfaces import PropComparator


Base = declarative_base()

def get_sql_type(value):
    if isinstance(value, bool):
        return 'bool'
    if np.issubdtype(type(value), np.floating):
        return 'float'
    if np.issubdtype(type(value), np.integer):
        return 'int'
    if isinstance(value, str):
        return 'str'
    if isinstance(value, datetime):
        return 'datetime'
    raise TypeError(f"Value: {value}, with type {type(value)} is not a valid type for sql (int, float, bool, str, datetime).")

def cast_sql_type(value):
    if isinstance(value, bool):
        return value
    if np.issubdtype(type(value), np.floating):
        return float(value)
    if np.issubdtype(type(value), np.integer):
        return int(value)
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.timestamp()
    raise TypeError(f"Value: {value}, with type {type(value)} is not a valid type for sql (int, float, bool, str, datetime).")

def get_sql_schema(sql_data):
    schema = {}
    for k, v in sql_data.items():
        schema[k] = get_sql_type(v)
    return schema


def create_tables(engine):
    """Create all Journey cache tables (path, path_version, result).
    Safe to call anytime: only creates tables that do not already exist (checkfirst=True).
    """
    Base.metadata.create_all(engine)


class DBPath(Base):
    __tablename__ = "path"
    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    current_version = Column(Integer, nullable=True)
    versions = relationship("DBPathVersion", back_populates="path")


class DBPathVersion(Base):
    __tablename__ = "path_version"

    version = Column(Integer, primary_key=True)
    name = Column(String, ForeignKey('path.name'), primary_key=True, nullable=False)
    changelog = Column(String, nullable=True)

    path = relationship("DBPath", back_populates="versions")
    results = relationship("DBResult", back_populates="path_version")
    env_schema = Column(JSONB, nullable=False)
    file_schema = Column(JSONB, nullable=True)


class DBResult(Base):
    __tablename__ = "result"

    id = Column(Integer, primary_key=True)
    environment = Column(JSONB, nullable=False)
    data = Column(JSONB, nullable=True)
    file_path = Column(String, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=True)

    # Relationship back to PathVersion
    path_version = relationship("DBPathVersion", back_populates="results")
    # Foreign key columns to PathVersion
    path_name = Column(String, nullable=False)
    path_version_num = Column(Integer, nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
            ['path_name', 'path_version_num'],
            ['path_version.name', 'path_version.version']
        ),
    )

