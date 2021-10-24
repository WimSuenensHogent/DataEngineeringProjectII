import pandas as pd

from sqlalchemy.orm import Session

from app.etl.transformer import CommonTransformer
from app.exceptions import IncorrectDataSourcePath
from app.models.models import Base


class Pipeline:
    def __init__(
        self,
        data_class: Base,
        path: str,
        transformer: CommonTransformer,
        # session: Session,
    ):
        # self.session = session
        self.data_class = data_class
        self.path = path
        self.transformer = transformer

    def extract(self):
        """Haalt data op van url

        Returns:
            pd.DataFrame: [description]
        """
        if ".csv" in self.path:
            data_frame = pd.read_csv(self.path)
        elif ".xlsx" in self.path:
            data_frame = pd.read_excel(self.path, nrows=50)
        elif ".zip" in self.path:
            data_frame = pd.read_csv(self.path, delimiter="|")
        else:
            raise IncorrectDataSourcePath
        return data_frame

    def transform(self, data_frame: pd.DataFrame):
        """Transformeert de data in correct formaat

        Args:
            data_frame (pd.DataFrame): [description]

        Returns:
            [type]: [description]
        """
        return self.transformer.transform(data_frame, self.path)

    def load(self, session: Session, data_frame: pd.DataFrame):
        # if we dont want data to fully load again in the local DB,
        # better to compare first and only add a new chunk
        # dropping the existins size of new table
        count_rows_in_db = session.query(self.data_class.id).count()
        print(f"Rows in outsourced table db: {count_rows_in_db}")
        count_rows_in_updated_data_source = len(data_frame.index)
        print(
            f"Rows in current {self.data_class.__tablename__} db: {count_rows_in_updated_data_source}"
        )
        data_frame = data_frame.iloc[
            count_rows_in_updated_data_source
            - (count_rows_in_updated_data_source - count_rows_in_db) :
        ]
        list = [
            self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
        ]
        # session.add_all(list)
        session.bulk_save_objects(list)
        session.commit()

    def process(self, session: Session):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_list = self.load(session, data_frame)
        return data_list
