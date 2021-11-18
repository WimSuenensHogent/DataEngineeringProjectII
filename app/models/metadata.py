from app.models.base import Base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Date, String


class ETL_Metadata(Base):
    __tablename__ = "etl_metadata"

    table = Column(String(255), primary_key=True)
    last_date_processed = Column(Date, nullable=False)

    def __init__(self, table, last_date_processed):
        self.table = table
        self.last_date_processed = last_date_processed
