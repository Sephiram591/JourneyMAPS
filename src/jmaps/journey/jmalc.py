from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime,
    ForeignKey, Table, Enum, UniqueConstraint, Text
)
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()

# ---------------- ENUM for Param Data Type ---------------- #
class DataType(enum.Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOL = "bool"
    DATETIME = "datetime"

# ---------------- Association Table for Many-to-Many ---------------- #
pathresult_environment_table = Table(
    'pathresult_environment', Base.metadata,
    Column('pathresult_id', Integer, ForeignKey('path_results.id'), primary_key=True),
    Column('environment_instance_id', Integer, ForeignKey('environment_instances.id'), primary_key=True)
)

# ---------------- PARAMS ---------------- #
class Param(Base):
    __tablename__ = 'params'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    data_type = Column(Enum(DataType), nullable=False)
    int_value = Column(Integer)
    float_value = Column(Float)
    string_value = Column(String)
    bool_value = Column(Boolean)
    datetime_value = Column(DateTime)

    environment_instance_id = Column(Integer, ForeignKey('environment_instances.id'))
    environment_instance = relationship("EnvironmentInstance", back_populates="params")

    __table_args__ = (
        UniqueConstraint('environment_instance_id', 'name', name='uq_param_name_per_env_instance'),
    )

# ---------------- ENVIRONMENT & ENVIRONMENT INSTANCES ---------------- #
class Environment(Base):
    __tablename__ = 'environments'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    instances = relationship("EnvironmentInstance", back_populates="environment")

class EnvironmentInstance(Base):
    __tablename__ = 'environment_instances'

    id = Column(Integer, primary_key=True)
    environment_id = Column(Integer, ForeignKey('environments.id'))
    environment = relationship("Environment", back_populates="instances")

    params = relationship("Param", back_populates="environment_instance", cascade="all, delete-orphan")

    path_results = relationship(
        "PathResult",
        secondary=pathresult_environment_table,
        back_populates="environment_instances"
    )

# ---------------- JOURNEY / PATHS / PATHVERSIONS ---------------- #
class Journey(Base):
    __tablename__ = 'journeys'

    name = Column(String, unique=True, nullable=False, primary_key=True)

    paths = relationship("Path", back_populates="journey")
    __table_args__ = (
        UniqueConstraint('name', name='uq_journey_name'),
    )

class Path(Base):
    __tablename__ = 'paths'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    journey_name = Column(String, ForeignKey('journeys.name'))
    journey = relationship("Journey", back_populates="paths")

    path_versions = relationship("PathVersion", back_populates="path")
    __table_args__ = (
        UniqueConstraint('journey_name', 'name', name='uq_path_name_per_journey'),
    )

class PathVersion(Base):
    __tablename__ = 'path_versions'

    id = Column(Integer, primary_key=True)
    version_filepath = Column(Text, nullable=False)

    path_id = Column(Integer, ForeignKey('paths.id'))
    path = relationship("Path", back_populates="path_versions")

    path_results = relationship("PathResult", back_populates="path_version")
    __table_args__ = (
        UniqueConstraint('path_id', 'version_filepath', name='uq_path_version_filepath'),
    )

class PathResult(Base):
    __tablename__ = 'path_results'

    id = Column(Integer, primary_key=True)
    result_filepath = Column(Text, nullable=False)

    path_version_id = Column(Integer, ForeignKey('path_versions.id'))
    path_version = relationship("PathVersion", back_populates="path_results")

    environment_instances = relationship(
        "EnvironmentInstance",
        secondary=pathresult_environment_table,
        back_populates="path_results"
    )
