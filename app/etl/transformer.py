from abc import ABC

import pandas as pd


class CommonTransformer(ABC):
    def __init__(
        self,
        column_renamer: dict = None,
        na_remover: bool = True,
        drop_columns: list = None,
    ):
        self.column_renamer = column_renamer
        self.na_remover = na_remover
        self.drop_columns = drop_columns

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
            na_remover=True,
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
        )

    def custom_transform(self, data_frame: pd.DataFrame, path):
        year = self.extract_year_from_path(path)
        data_frame["year"] = year
        return data_frame
