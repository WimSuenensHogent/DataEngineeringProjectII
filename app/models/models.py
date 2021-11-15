"""[summary]

Returns:
    [type]: [description]
"""
import os

import numpy as np
import pandas as pd
from datetime import datetime, date

from pandas.core.series import Series
from app.models.base import Base
from app.utils import db_session
from sqlalchemy.orm import declarative_base, relationship, backref, validates
from sqlalchemy.sql.sqltypes import Boolean, Date, Integer, String
from sqlalchemy.sql.schema import CheckConstraint, Column, ForeignKey
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

            if (len(local_nis_codes_all)):
                return local_nis_codes_all

        # extract
        # https://statbel.fgov.be/sites/default/files/files/opendata/REFNIS%20code/TU_COM_REFNIS.xlsx
        zip_url = "https://statbel.fgov.be/sites/default/files/files/opendata/REFNIS%20code/TU_COM_REFNIS.zip"
        data_frame = pd.read_csv(zip_url, delimiter="|")
        
        # transform
        data_frame['CD_REFNIS'] = data_frame['CD_REFNIS'].apply(lambda x: '{0:0>5}'.format(x))
        data_frame['CD_SUP_REFNIS'] = data_frame['CD_SUP_REFNIS'].apply(lambda x: x if (len(x) == 5) else None)
        data_frame["DT_VLDT_START"] = np.where(
            data_frame['DT_VLDT_START'] == '01/01/1970',
            date.min,
            data_frame['DT_VLDT_START'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date())
        )
        data_frame["DT_VLDT_END"] = np.where(
            data_frame['DT_VLDT_END'] == '31/12/9999',
            date.max,
            data_frame['DT_VLDT_END'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').date())
        )
        data_frame = data_frame[(data_frame["DT_VLDT_END"] == date.max)]
        data_frame.rename(columns={
            "LVL_REFNIS": "level",
            "CD_REFNIS": "nis",
            "CD_SUP_REFNIS": "parent_nis",
            "TX_REFNIS_NL": "text_nl",
            "TX_REFNIS_FR": "text_fr",
            "TX_REFNIS_DE": "text_de",
            "DT_VLDT_START": "valid_from",
            "DT_VLDT_END": "valid_till",
        }, inplace=True)

        # load
        objects_list = [
            cls(**kwargs) for kwargs in (data_frame).to_dict(orient="records")
        ]
        with db_session(echo=False) as session:
            session.bulk_save_objects(objects_list)
            session.commit()
            session.close()
        return objects_list

class CovidVaccinationByCategory(Base):
    __tablename__ = "fact_covid_vaccinations_by_category"
    # id = Column(Integer, primary_key=True, nullable=False)
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
    # id = Column(Integer, primary_key=True, nullable=False)
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
    # id = Column(Integer, primary_key=True, nullable=False)
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

    # id = Column(Integer, primary_key=True, nullable=False)
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
    # id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date, primary_key=True)
    # year = Column(Integer, primary_key=True)
    # week = Column(String, primary_key=True)
    nis_district = Column(String, primary_key=True)
    sex = Column(String, primary_key=True)
    agegroup = Column(String, primary_key=True)
    number_of_deaths = Column(Integer, nullable=False)


class VaccinationsByNISCodeDailyUpdated(Base):
    __tablename__ = "fact_vaccinations_by_nis_code_daily_updated"

    # id = Column(Integer, primary_key=True, nullable=False)
    # date = Column(Date, nullable=False)
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


# class WekelijkseVaccinatiesPerNISCode(Base):
#     __tablename__ = "weekly_vaccinations_update_by_nic_code"

#     id = Column(Integer, primary_key=True, nullable=False)
#     date = Column(Date, nullable=False)
#     nis_code = Column(Integer, nullable=False)
#     agegroup = Column(String, nullable=False)
#     dose = Column(String, nullable=False)
#     cumul_of_week = Column(String, nullable=False)


# class LastPipeLineProcessing(Base):
#     __tablename__ = "meta_last_processing_date"
    
#     id = Column(Integer, primary_key=True, nullable=False)
#     date = Column(Date, nullable=False)

