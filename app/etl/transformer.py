from abc import ABC

import pandas as pd


class CommonTransformer(ABC):
    def __init__(self, column_renamer: dict = None, na_remover: bool = True):
        self.column_renamer = column_renamer
        self.na_remover = na_remover

    def custom_transform(self, data_frame: pd.DataFrame):
        """
        Deze method laat toe om custom transform acties te doen indien nodig.
        Overwrite deze method in de subclass indien gewenst anders zal deze gewoon
        de data teruggeven onveranderd.

        Args:
            data_frame: input data frame

        Returns: output data frame

        """
        return data_frame

    def transform(self, data_frame: pd.DataFrame):
        """Transformeert input df en returnt output df

        Args:
            data_frame (pd.DataFrame): input df

        Returns: output df
        """
        if self.column_renamer:
            data_frame = data_frame.rename(columns=self.column_renamer)

        if self.na_remover:
            data_frame = data_frame.dropna()

        return self.custom_transform(data_frame)


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
