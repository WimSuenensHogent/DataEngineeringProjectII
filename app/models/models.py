"""[summary]

Returns:
    [type]: [description]
"""
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Date
from sqlalchemy.sql.sqltypes import Integer
from sqlalchemy.sql.sqltypes import String

Base = declarative_base()


class CovidVaccinationByCategory(Base):
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


class CovidMortality(Base):
    __tablename__ = "covid_mortality"
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date)
    region = Column(String)
    agegroup = Column(String)
    sex = Column(String)
    deaths = Column(Integer)

    def __repr__(self):
        return """
            <BelgiumCovidMortality(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )


class CovidConfirmedCases(Base):
    __tablename__ = "covid_confirmed_cases"
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date)
    province = Column(String)
    region = Column(String)
    agegroup = Column(String)
    sex = Column(String)
    cases = Column(Integer)

    def __repr__(self):
        return """
            <BelgiumCovidConfirmedCases(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )
