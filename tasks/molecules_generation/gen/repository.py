from __future__ import annotations

import enum
import os
import uuid
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    relationship,
    sessionmaker,
)
from sqlalchemy.sql import func


class TaskStatus(str, enum.Enum):
    CREATED = "created"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class Base(DeclarativeBase):
    pass


class Task(Base):
    __tablename__ = "tasks"

    id = Column(String(36), primary_key=True)
    task_type = Column(String(64), nullable=False)
    status = Column(Enum(
        TaskStatus,
        name="task_status",
        values_callable=lambda x: [e.value for e in x]),
        nullable=False,
        default=TaskStatus.CREATED
    )
    params = Column(JSONB, nullable=False, default=dict)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    artifacts = relationship("Artifact", back_populates="task", lazy="select")


class Artifact(Base):
    __tablename__ = "artifacts"

    id = Column(String(36), primary_key=True)
    task_id = Column(String(36), ForeignKey("tasks.id"), nullable=False)
    s3_key = Column(String(512), nullable=False)
    filename = Column(String(256), nullable=False)
    content_type = Column(String(128), nullable=True)
    meta = Column(JSONB, nullable=True, default=dict)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    task = relationship("Task", back_populates="artifacts")


def _build_engine():
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


_SessionLocal = None


def _get_session_factory() -> sessionmaker:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=_build_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _SessionLocal


@contextmanager
def get_session() -> Generator[Session, None, None]:
    session: Session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class TaskRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_id(self, task_id: str) -> Task:
        task = self._session.get(Task, task_id)
        if task is None:
            raise ValueError(f"Task '{task_id}' not found.")
        return task

    def get_artifact_by_id(self, artifact_id: str) -> Artifact:
        artifact = self._session.get(Artifact, artifact_id)
        if artifact is None:
            raise ValueError(f"Artifact '{artifact_id}' not found.")
        return artifact

    def mark_running(self, task: Task) -> None:
        task.status = TaskStatus.RUNNING
        task.error = None
        self._session.add(task)

    def mark_done(self, task: Task) -> None:
        task.status = TaskStatus.SUCCESS
        self._session.add(task)

    def mark_failed(self, task: Task, error: str) -> None:
        task.status = TaskStatus.FAILED
        task.error = error
        self._session.add(task)

    def save_molecules_file(
        self,
        task: Task,
        s3_key: str,
        filename: str,
        content_type: str = "csv/text",
        meta: dict | None = None,
    ) -> Artifact:
        artifact = Artifact(
            id=str(uuid.uuid4()),
            task_id=task.id,
            s3_key=s3_key,
            filename=filename,
            content_type=content_type,
            meta=meta or {},
        )
        self._session.add(artifact)
        return artifact
