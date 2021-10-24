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
    date = Column(Date, nullable=False)
    region = Column(String, nullable=False)
    agegroup = Column(String, nullable=False)
    sex = Column(String, nullable=False)
    brand = Column(String, nullable=False)
    dose = Column(String, nullable=False)
    count = Column(Integer, nullable=False)

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
    date = Column(Date, nullable=False)
    region = Column(String, nullable=False)
    agegroup = Column(String, nullable=False)
    sex = Column(String, nullable=False)
    deaths = Column(Integer, nullable=False)

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
    date = Column(Date, nullable=False)
    province = Column(String, nullable=False)
    region = Column(String, nullable=False)
    agegroup = Column(String, nullable=False)
    sex = Column(String, nullable=False)
    cases = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <BelgiumCovidConfirmedCases(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )


class RegionDemographics(Base):
    __tablename__ = "region_demographics"
    id = Column(Integer, primary_key=True, nullable=False)
    year = Column(Integer, nullable=True)
    municipality_niscode = Column(Integer, nullable=True)
    municipality_name = Column(String, nullable=True)
    district_niscode = Column(Integer, nullable=True)
    district_name = Column(String, nullable=True)
    province_niscode = Column(Integer, nullable=True)
    province_name = Column(String, nullable=True)
    region_niscode = Column(Integer, nullable=True)
    region_name = Column(String, nullable=True)
    sex = Column(String, nullable=True)
    nationality_code = Column(String, nullable=True)
    nationality_name = Column(String, nullable=True)
    marital_status_code = Column(String, nullable=True)
    marital_status_name = Column(String, nullable=True)
    age = Column(Integer, nullable=True)
    population = Column(Integer, nullable=True)


class TotalNumberOfDeadsPerRegions(Base):
    __tablename__ = "total_number_of_deads_per_region"
    id = Column(Integer, primary_key=True, nullable=False)
    district_niscode = Column(String, nullable=False)
    province_niscode = Column(String, nullable=False)
    region_niscode = Column(String, nullable=False)
    sex = Column(String, nullable=False)
    agegroup = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    weak = Column(String, nullable=False)
    number_of_deaths = Column(Integer, nullable=False)
