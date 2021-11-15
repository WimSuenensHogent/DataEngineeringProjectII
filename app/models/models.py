"""[summary]

Returns:
    [type]: [description]
"""
import os

from app.models.base import Base
from app.utils import db_session
from sqlalchemy.orm import validates
from sqlalchemy.sql.sqltypes import Boolean, Date, Integer, String
from sqlalchemy.sql.schema import CheckConstraint, Column
# https://docs.sqlalchemy.org/en/14/orm/self_referential.html

class NIS_Code(Base):
    __tablename__ = 'dim_nis_codes'
    
    nis = Column(String(5), primary_key=True)
    # parent_nis = Column(String(5), ForeignKey('dim_nis_codes.nis'), nullable=True)
    parent_nis = Column(String(5), nullable=True)
    level = Column(Integer, nullable=False)
    text_nl = Column(String(255))
    text_fr = Column(String(255))
    text_de = Column(String(255))
    valid_from = Column(Date, primary_key=True)
    valid_till = Column(Date, nullable=False)
    
    # children = relationship(
    #     "NIS_Code",
    #     lazy='select',                    
    #     backref=backref('parent', remote_side=[nis])
    # )

    __table_args__ = tuple(
        (
            CheckConstraint('LEN(nis)=5'),
            CheckConstraint('LEN(parent_nis)=5')
        ) if (
            os.environ.get('DATABASE_URL')
        ) else (
            CheckConstraint('length(nis)==5'),
            CheckConstraint('length(parent_nis)==5')
        ),
    )
    
    def __repr__(self):
        return """
            <NIS_Code(level='%s', nis='%s', name='%s', parent='%s')>
            """ % (
            self.level,
            self.nis,
            self.text_nl,
            self.parent_nis
        )

    @validates('nis')
    def validate_nis(self, key, nis) -> str:
        if len(nis) != 5:
            raise ValueError("'nis' should be 5 characters long..")
        return nis
    
    @validates('parent_nis')
    def validate_parent_nis(self, key, parent_nis) -> str:
        if (not parent_nis):
            return parent_nis
        if len(parent_nis) != 5:
            raise ValueError("'parent_nis' should be 5 characters long..")
        return parent_nis

    @classmethod
    def get_all(cls):
        with db_session(echo=False) as session:
            local_nis_codes_all = session.query(NIS_Code).all()
            session.close()
            return local_nis_codes_all
class CovidVaccinationByCategory(Base):
    __tablename__ = "fact_covid_vaccinations_by_category"

    date = Column(Date, primary_key=True, nullable=False)
    region = Column(String, primary_key=True, nullable=False)
    agegroup = Column(String, primary_key=True, nullable=False)
    sex = Column(String, primary_key=True, nullable=False)
    brand = Column(String, primary_key=True, nullable=False)
    dose = Column(String, primary_key=True, nullable=False)
    count = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <CovidVaccinationByCategory(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )


class CovidMortalityByCategory(Base):
    __tablename__ = "fact_covid_mortality_by_category"

    date = Column(Date, primary_key=True)
    region = Column(String, primary_key=True)
    agegroup = Column(String, primary_key=True)
    sex = Column(String, primary_key=True)
    deaths = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <CovidMortalityByCategory(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )
class CovidConfirmedCasesByCategory(Base):
    __tablename__ = "fact_covid_confirmed_cases_by_category"

    date = Column(Date, primary_key=True)
    province = Column(String, primary_key=True)
    region = Column(String, primary_key=True)
    agegroup = Column(String, primary_key=True)
    sex = Column(String, primary_key=True)
    cases = Column(Integer, nullable=False)

    def __repr__(self):
        return """
            <CovidConfirmedCasesByCategory(date='%s', region='%s', agegroup='%s')>
            """ % (
            self.date,
            self.region,
            self.agegroup,
        )

class DemographicsByNISCodeAndCategory(Base):
    __tablename__ = "fact_demographics_by_nis_code_and_category"

    year = Column(Integer, primary_key=True)
    nis = Column(Integer, primary_key=True)
    sex = Column(String, primary_key=True)
    nationality_code = Column(String, primary_key=True)
    nationality_text_nl = Column(String)
    nationality_text_fr = Column(String)
    marital_status_code = Column(String, primary_key=True)
    marital_status_text_nl = Column(String)
    marital_status_text_fr = Column(String)
    age = Column(Integer, primary_key=True)
    population = Column(Integer)


class NumberOfDeathsByDistrictNISCode(Base):
    __tablename__ = "fact_number_of_deaths_by_district_nis_code"

    date = Column(Date, primary_key=True)
    nis_district = Column(String, primary_key=True)
    sex = Column(String, primary_key=True)
    agegroup = Column(String, primary_key=True)
    number_of_deaths = Column(Integer, nullable=False)


class VaccinationsByNISCodeDailyUpdated(Base):
    __tablename__ = "fact_vaccinations_by_nis_code_daily_updated"

    nis_code = Column(String(5), primary_key=True)
    sex = Column(String, primary_key=True)
    agegroup = Column(String, primary_key=True)
    plus18 = Column(Boolean, primary_key=True)
    plus65 = Column(Boolean, primary_key=True)
    vaccinated_fully_total = Column(Integer, nullable=False)
    vaccinated_partly_total = Column(Integer, nullable=False)
    vaccinated_fully_astrazeneca = Column(Integer, nullable=False)
    vaccinated_partly_astrazeneca = Column(Integer, nullable=False)
    vaccinated_fully_pfizer = Column(Integer, nullable=False)
    vaccinated_partly_pfizer = Column(Integer, nullable=False)
    vaccinated_fully_moderna = Column(Integer, nullable=False)
    vaccinated_partly_moderna = Column(Integer, nullable=False)
    vaccinated_fully_johnsonandjohnson = Column(Integer, nullable=False)
    vaccinated_fully_other = Column(Integer, nullable=False)
    vaccinated_partly_other = Column(Integer, nullable=False)
    population_by_agecategory_and_municipality = Column(Integer, nullable=False)
class VaccinationsByNISCodeAndWeek(Base):
    __tablename__ = "fact_vaccinations_by_nis_code_and_week"

    date = Column(Date, primary_key=True)
    year = Column(Integer, primary_key=True)
    week = Column(Integer, primary_key=True)
    nis_code = Column(String(5), primary_key=True)
    agegroup = Column(String(5), primary_key=True)
    dose = Column(String(5), primary_key=True)
    cumul_of_week = Column(Integer, nullable=False)

    __table_args__ = tuple(
        (
            (CheckConstraint('LEN(nis_code)=5'),
        ) if (os.environ.get('DATABASE_URL')) else (
            CheckConstraint('length(nis_code)==5')),
        )
    )

    @validates('nis_code')
    def validate_nis_code(self, key, nis_code) -> str:
        if len(nis_code) != 5:
            raise ValueError("'nis_code' should be 5 characters long..")
        return nis_code

