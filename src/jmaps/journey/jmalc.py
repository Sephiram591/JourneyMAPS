"""SQLAlchemy models and helpers for JourneyMAPS caching.

This module defines the ORM tables used to persist path definitions and
results, as well as small utilities to map Python values to SQL-friendly
types and schemas.
"""

import numpy as np
from datetime import datetime
from sqlalchemy import (
    TIMESTAMP,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    String,
    Boolean,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from collections.abc import Sequence


Base = declarative_base()

def get_sql_type(value):
    """Return the canonical SQL type name for a Python value.

    Args:
        value: Value to inspect.

    Returns:
        str: One of ``"bool"``, ``"float"``, ``"int"``, ``"str"`` or ``"datetime"``.

    Raises:
        TypeError: If the value cannot be represented as a supported SQL type.
    """
    if isinstance(value, bool):
        return "bool"
    if np.issubdtype(type(value), np.floating):
        return "float"
    if np.issubdtype(type(value), np.integer):
        return "int"
    if isinstance(value, str):
        return "str"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return type(value).__name__
    raise TypeError(
        f"Value: {value}, with type {type(value)} is not a valid type for sql "
        "(int, float, bool, str, datetime, Sequence)."
    )

def cast_sql_type(value):
    """Cast a Python value to a JSON-/SQL-serializable primitive.

    The mapping mirrors :func:`get_sql_type`:

    * ``bool``  → ``bool``
    * float-like → ``float``
    * int-like → ``int``
    * ``str``  → ``str``
    * ``datetime`` → POSIX timestamp (``float`` in seconds)

    Args:
        value: Value to cast.

    Returns:
        bool | float | int | str: Cast primitive representation.

    Raises:
        TypeError: If the value cannot be represented as a supported SQL type.
    """
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
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [cast_sql_type(v) for v in value]
    raise TypeError(
        f"Value: {value}, with type {type(value)} is not a valid type for sql "
        "(int, float, bool, str, datetime)."
    )

def is_sql_type(value):
    """Check if a Python value can be represented as a SQL type.

    Args:
        value: Value to check.

    Returns:
        bool: True if the value can be represented as a SQL type, False otherwise.
    """
    try:
        get_sql_type(value)
        return True
    except TypeError:
        return False


def create_tables(engine):
    """Create all Journey cache tables on the given engine.

    This is safe to call repeatedly; tables are only created if they do not
    already exist.

    Args:
        engine: SQLAlchemy :class:`Engine` bound to the target database.
    """
    Base.metadata.create_all(engine)


class DBPath(Base):
    """ORM model for a logical Journey path definition.

    Each row corresponds to a named path and tracks the currently active
    version stored in :class:`DBPathVersion`.
    """

    __tablename__ = "path"

    name = Column(String, primary_key=True)
    description = Column(String, nullable=True)
    versions = relationship("DBPathVersion", back_populates="path")


class DBPathVersion(Base):
    """ORM model for a concrete version of a path.

    Versions are keyed by ``(name, version)`` and store the environment and
    file schemas used to interpret cached results.
    """

    __tablename__ = "path_version"

    version = Column(Integer, primary_key=True)
    name = Column(String, ForeignKey("path.name"), primary_key=True, nullable=False)
    changelog = Column(String, nullable=True)

    path = relationship("DBPath", back_populates="versions")
    results = relationship("DBResult", back_populates="path_version")
    env_schema = Column(JSONB, nullable=False)
    file_schema = Column(JSONB, nullable=True)
    ast_hash = Column(LargeBinary, nullable=False)
    nondeterministic = Column(Boolean, nullable=False, default=False)


class DBResult(Base):
    """ORM model for a single cached path execution result."""

    __tablename__ = "result"

    id = Column(Integer, primary_key=True)
    environment = Column(JSONB, nullable=False)
    data = Column(JSONB, nullable=True)
    file_name = Column(String, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=True)
    completed = Column(Boolean, nullable=False)

    # Relationship back to PathVersion
    path_version = relationship("DBPathVersion", back_populates="results")

    # Foreign key columns to PathVersion
    path_name = Column(String, nullable=False)
    path_version_num = Column(Integer, nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
            ["path_name", "path_version_num"],
            ["path_version.name", "path_version.version"],
        ),
    )

