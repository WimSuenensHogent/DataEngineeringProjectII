import pandas as pd

class Transformer():

    def __init__(self, column_renamer: dict = None):
        self.column_renamer = column_renamer

    def transform_data_frame(self, data_frame: pd.DataFrame):
        """[summary]

        Args:
            data_frame (pd.DataFrame): [description]

        Returns:
            [type]: [description]
        """
        if self.column_renamer :
            data_frame = data_frame.rename(columns=self.column_renamer)

        return data_frame.head(5)
