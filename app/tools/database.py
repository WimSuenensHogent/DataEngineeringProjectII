"""[summary]
"""
from sqlalchemy import create_engine

from app.models.models import Base

class Database:
    """[summary]
    """
    def __init__(self, base_class: Base):
        self.base_class = base_class

        server='34.140.20.123'
        database='TestDB'
        driver='ODBC Driver 17 for SQL Server'
        user='SA'
        password='SQL4HoGent'
        self.database_con = f'mssql+pyodbc://{user}:{password}@{server}:1433/{database}?driver={driver}'
        # self.engine = create_engine(f'mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}')
        self.engine = create_engine(self.database_con, echo=True)
        self.connection = self.engine.connect

    def run_migrations(self):
        self.base_class.metadata.create_all(self.engine)

    def get_engine(self):
        """[summary]
        """
        return self.engine

    def get_connection(self):
        """[summary]
        """
        return self.connection
