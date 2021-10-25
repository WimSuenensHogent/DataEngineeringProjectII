"""[summary]

Returns:
    [type]: [description]
"""
import numpy as np
import pandas as pd
from datetime import datetime, date
from app.utils import db_session
from sqlalchemy.orm import declarative_base, relationship, backref, validates
from sqlalchemy.sql.sqltypes import Date, Integer, String
from sqlalchemy.sql.schema import CheckConstraint, Column, ForeignKey
# https://docs.sqlalchemy.org/en/14/orm/self_referential.html

Base = declarative_base()

class NSI_Code(Base):
    __tablename__ = 'dim_nsi_codes'
    
    nsi = Column(String(5), primary_key=True)
    parent_nsi = Column(String(5), ForeignKey('dim_nsi_codes.nsi'), nullable=True)
    level = Column(Integer, nullable=False)
    text_nl = Column(String(255))
    text_fr = Column(String(255))
    text_de = Column(String(255))
    valid_from = Column(Date, primary_key=True)
    valid_till = Column(Date, nullable=True)
    
    children = relationship("NSI_Code",
        lazy='select',                    
        backref=backref('parent', remote_side=[nsi])
    )
    
    __table_args__ = (
        CheckConstraint('length(nsi) = 5', name='nsi_length'),
    )
    
    def __repr__(self):
        return """
            <NSI_Code(level='%s', nsi='%s', name='%s', parent='%s')>
            """ % (
            self.level,
            self.nsi,
            self.text_nl,
            self.parent_nsi
        )

    @validates('nsi')
    def validate_nsi(self, key, nsi) -> str:
        if len(nsi) != 5:
            raise ValueError("'nsi' should be 5 characters long..")
        return nsi
    
    @classmethod
    def get_all(cls):
        with db_session(echo=False) as session:
            local_nsi_codes_all = session.query(NSI_Code).all()
            session.close()
            if (len(local_nsi_codes_all)):
                return local_nsi_codes_all

        # extract
        # https://statbel.fgov.be/sites/default/files/files/opendata/REFNIS%20code/TU_COM_REFNIS.xlsx
        zip_url = "https://statbel.fgov.be/sites/default/files/files/opendata/REFNIS%20code/TU_COM_REFNIS.zip"
        data_frame = pd.read_csv(zip_url, delimiter="|")
        
        # transform
        data_frame['CD_REFNIS'] = data_frame['CD_REFNIS'].apply(lambda x: '{0:0>5}'.format(x))
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
        data_frame.rename(columns={
            "LVL_REFNIS": "level",
            "CD_REFNIS": "nsi",
            "CD_SUP_REFNIS": "parent_nsi",
            "TX_REFNIS_NL": "text_nl",
            "TX_REFNIS_FR": "text_fr",
            "TX_REFNIS_DE": "text_de",
            "DT_VLDT_START": "valid_from",
            "DT_VLDT_END": "valid_till",
        }, inplace=True)

        # load
        objects_list = [
            cls(**kwargs) for kwargs in data_frame.to_dict(orient="records")
        ]
        with db_session(echo=False) as session:
            session.bulk_save_objects(objects_list)
            session.commit()
            session.close()
        return objects_list
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
