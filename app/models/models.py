"""[summary]

Returns:
    [type]: [description]
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import String, Integer, Date

Base = declarative_base()
engine = create_engine(
    "mssql://sa:SQL4HoGent@34.140.20.123/test_tim?driver=ODBC Driver 17 for SQL Server"
)


class CovidVacinationByCategory(Base):
    __tablename__ = "covid_vaccinations_by_category"
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date)
    region = Column(String)
    agegroup = Column(String)
    sex = Column(String)
    brand = Column(String)
    dose = Column(String)
    count = Column(Integer)

    def __repr__(self):
        return """
            <BelgiumVacinationByCategory(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )
