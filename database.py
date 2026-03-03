from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from config import settings

if not settings.STAGING_DB.parent.exists():
    settings.STAGING_DB.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(f"sqlite:///{settings.STAGING_DB}")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class Timepoint(Base):
    __tablename__ = "timepoints"

    series_id: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(primary_key=True)
    value: Mapped[float] = mapped_column()


def init_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def close_db():
    engine.dispose()
