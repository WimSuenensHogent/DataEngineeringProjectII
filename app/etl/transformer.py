from abc import ABC

import pandas as pd


class CommonTransformer(ABC):
    def __init__(
        self,
        column_renamer: dict = None,
        na_remover: bool = True,
        drop_columns: list = None,
        na_filler: bool = False
    ):
        self.column_renamer = column_renamer
        self.na_remover = na_remover
        self.drop_columns = drop_columns
        self.na_filler = na_filler

    def custom_transform(self, data_frame: pd.DataFrame, path):
        """
        Deze method laat toe om custom transform acties te doen indien nodig.
        Overwrite deze method in de subclass indien gewenst anders zal deze gewoon
        de data teruggeven onveranderd.

        Args:
            data_frame: input data frame

        Returns: output data frame

        """
        print(data_frame.dtypes)
        return data_frame

    def transform(self, data_frame: pd.DataFrame, path: str):
        """Transformeert input df en returnt output df

        Args:
            data_frame (pd.DataFrame): input df

        Returns: output df
        """
        if self.drop_columns:
            data_frame.drop(self.drop_columns, axis=1, inplace=True)
        if self.column_renamer:
            data_frame.rename(columns=self.column_renamer, inplace=True)
        if self.na_remover:
            data_frame.dropna(inplace=True)
        if self.na_filler:
            data_frame.fillna(value=0, inplace=True)

        return self.custom_transform(data_frame, path)

    def extract_year_from_path(self, path):
        return path.split(".")[-2][-4:]


class TransformCovidVaccinationByCategory(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=True,
            column_renamer={
                "DATE": "date",
                "REGION": "region",
                "AGEGROUP": "agegroup",
                "SEX": "sex",
                "BRAND": "brand",
                "DOSE": "dose",
                "COUNT": "count",
            },
        )


class TransformCovidMortality(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=True,
            column_renamer={
                "DATE": "date",
                "REGION": "region",
                "AGEGROUP": "agegroup",
                "SEX": "sex",
                "DEATHS": "deaths",
            },
        )


class TransformCovidConfirmedCases(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=True,
            column_renamer={
                "DATE": "date",
                "PROVINCE": "province",
                "REGION": "region",
                "AGEGROUP": "agegroup",
                "SEX": "sex",
                "CASES": "cases",
            },
        )


class TransformDemographicData(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=False, #Alle data uit Brussels gewest bevat NA's velden, als gevolg geen info over brussel
            column_renamer={
                "CD_REFNIS": "municipality_niscode",  # Gemeente nis
                "TX_DESCR_NL": "municipality_name",  # Gemeente
                "CD_DSTR_REFNIS": "district_niscode",  # Arrondissement nis
                "TX_ADM_DSTR_DESCR_NL": "district_name",  # Arrondissement
                "CD_PROV_REFNIS": "province_niscode",  # Provincie nis
                "TX_PROV_DESCR_NL": "province_name",  # Provincie
                "CD_RGN_REFNIS": "region_niscode",  # Gewest Nis
                "TX_RGN_DESCR_NL": "region_name",  # Gewest
                "CD_SEX": "sex",  # sex
                "CD_NATLTY": "nationality_code",  # nationaliteit
                "TX_NATLTY_NL": "nationality_name",
                "CD_CIV_STS": "marital_status_code",
                "TX_CIV_STS_NL": "marital_status_name",
                "CD_AGE": "age",
                "MS_POPULATION": "population",  # Aantal mensjes
            },
            drop_columns=[
                "TX_DESCR_FR",
                "TX_ADM_DSTR_DESCR_FR",
                "TX_PROV_DESCR_FR",
                "TX_RGN_DESCR_FR",
                "TX_NATLTY_FR",
                "TX_CIV_STS_FR",
            ],
            na_filler=True
        )

    def custom_transform(self, data_frame: pd.DataFrame, path):
        year = self.extract_year_from_path(path)
        data_frame["year"] = year
        return data_frame


class TransformTotalNumberOfDeadsPerRegion(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=True,
            column_renamer={
                "CD_ARR": "district_niscode",
                "CD_PROV": "province_niscode",
                "CD_REGIO": "region_niscode",
                "CD_SEX": "sex",
                "CD_AGEGROUP": "agegroup",
                "DT_DATE": "date",
                "NR_YEAR": "year",
                "NR_WEEK": "weak",
                "MS_NUM_DEATH": "number_of_deaths",
            },
        )

    def custom_transform(self, df: pd.DataFrame, path):
        df['date'] = pd.to_datetime(df['date'])
        return df

class TransformTotalNumberOfVaccinationsPerNICCode(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=False,
            column_renamer={
                "NIS_CD": "nis_code",
                "GENDER_CD": "sex",
                "AGE_CD": "agegroup",
                "ADULT_FL(18+)": "plus18",
                "SENIOR_FL(65+)": "plus65",
                "MUNICIPALITY": "municipality",
                "PROVINCE": "province",
                "REGION": "region",
                "EERSTELIJNSZONE": "eerstelijnzone",
                "FULLY_VACCINATED_AMT": "fully_vaccinated_in_total",
                "PARTLY_VACCINATED_AMT": "partly_vaccinated_in_total",
                "FULLY_VACCINATED_AZ_AMT": "fully_vaccinated_w_astrazeneca",
                "PARTLY_VACCINATED_AZ_AMT": "partly_vaccinated_w_astrazeneca",
                "FULLY_VACCINATED_PF_AMT": "fully_vaccinated_w_pfizer",
                "PARTLY_VACCINATED_PF_AMT": "partly_vaccinated_w_pfizer",
                "FULLY_VACCINATED_MO_AMT": "fully_vaccinated_w_moderna",
                "PARTLY_VACCINATED_MO_AMT": "partly_vaccinated_w_moderna",
                "FULLY_VACCINATED_JJ_AMT": "fully_vaccinated_w_jj",
                "FULLY_VACCINATED_OTHER_AMT": "fully_vaccinated_w_other",
                "PARTLY_VACCINATED_OTHER_AMT": "partly_vaccinated_w_other",
                "POPULATION_NBR": "population_per_agecategory_of_municipality",
            },
            na_filler=True
        )

    def custom_transform(self, df: pd.DataFrame, path):
        df.insert(loc=0, column='date', value=pd.to_datetime('today').date())
        return df

class TransformWekelijkseVaccinatiesPerNISCode(CommonTransformer):
    def __init__(self):
        super().__init__(
            na_remover=True,
            column_renamer={
                "YEAR_WEEK": "date",
                "NIS5": "nis_code",
                "AGEGROUP": "agegroup",
                "DOSE": "dose",
                "CUMUL": "cumul_of_week",
            },
        )

    def custom_transform(self, df: pd.DataFrame, path):
        df['date'] = pd.to_datetime(df['date'] + '3', format="%yW%W%w")
        return df
