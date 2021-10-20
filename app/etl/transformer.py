import pandas as pd


class Transformer:
    def __init__(self, column_renamer: dict = None,
                            na_remover: bool = True):
        self.column_renamer = column_renamer
        self.na_remover = na_remover

    def transform_data_frame(self, data_frame: pd.DataFrame):
        """[summary]

        Args:
            data_frame (pd.DataFrame): [description]

        Returns:
            [type]: [description]
        """
        if self.column_renamer:
            data_frame = data_frame.rename(columns=self.column_renamer)

        if self.na_remover:
            data_frame = data_frame.dropna()

        return data_frame
