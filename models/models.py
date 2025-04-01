from sqlalchemy import ForeignKey, Integer, BigInteger, String, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import List

class Base(DeclarativeBase):
    pass

class Country(Base):
    __tablename__ = "country"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    code: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)

    import_logs: Mapped[List["ImportLogs"]] = relationship(back_populates="country")
    api_import_logs: Mapped[List["ApiImportLogs"]] = relationship(back_populates="country")

class Api(Base):
    __tablename__ = "api"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)

    api_import_logs: Mapped[List["ApiImportLogs"]] = relationship(back_populates="api")

class ImportLogs(Base):
    __tablename__ = "import_logs"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    country_id: Mapped[int] = mapped_column(ForeignKey("country.id"), nullable=False)
    batch_timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    import_directory_name: Mapped[str] = mapped_column(String, nullable=True)
    import_file_name: Mapped[str] = mapped_column(String, nullable=True)
    file_created_date: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    file_last_modified_date: Mapped[DateTime] = mapped_column(DateTime, nullable=True)

    country: Mapped["Country"] = relationship(back_populates="import_logs")
    api_import_logs: Mapped[List["ApiImportLogs"]] = relationship(back_populates="import_logs")

class ApiImportLogs(Base):
    __tablename__ = "api_import_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    country_id: Mapped[int] = mapped_column(ForeignKey("country.id"), nullable=False)
    api_id: Mapped[int] = mapped_column(ForeignKey("api.id"), nullable=False)
    import_logs_id: Mapped[int] = mapped_column(ForeignKey("import_logs.id"), nullable=True)
    start_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    end_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    code_response: Mapped[int] = mapped_column(Integer, nullable=False)
    error_message: Mapped[str] = mapped_column(String, nullable=True)

    country: Mapped["Country"] = relationship(back_populates="api_import_logs")
    api: Mapped["Api"] = relationship(back_populates="api_import_logs")
    import_logs: Mapped["ImportLogs"] = relationship(back_populates="api_import_logs")