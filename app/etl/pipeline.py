import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from app.models.models import Base
from app.etl.transformer import Transformer


class Pipeline:
    def __init__(
        self, database, data_class: Base, csv_path: str, transformer: Transformer
    ):
        self.database = database
        self.data_class = data_class
        self.csv_path = csv_path
        self.transformer = transformer
        Session = sessionmaker(self.database.engine, expire_on_commit=False, autocommit=False)
        self.session = Session()

    def get_session(self):
        return self.session

    def extract(self):
        """[summary]

        Returns:
            pd.DataFrame: [description]
        """
        data_frame = pd.read_csv(self.csv_path)
        return data_frame

    def transform(self, data_frame: pd.DataFrame):
        """[summary]

        Args:
            data_frame (pd.DataFrame): [description]

        Returns:
            [type]: [description]
        """
        if self.transformer:
            data_frame = self.transformer.transform_data_frame(data_frame)
        return data_frame

    def load(self, data_frame: pd.DataFrame):

        if inspect(self.database.engine).has_table(self.data_class.__tablename__):
            self.database.run_migrations()
            print(">>> exists")
            #if we dont want data to fully load again in the local DB,
            #better to compare first and only add a new chunk
            #dropping the existins size of new table
            count_rows_in_db = self.get_session().query(self.data_class.id).count()
            print(f"Rows in outsourced table db: {count_rows_in_db}")
            count_rows_in_updated_data_source = len(data_frame.index)
            print(f"Rows in current {self.data_class.__tablename__} db: {count_rows_in_updated_data_source}")
            data_frame = data_frame.iloc[
                         count_rows_in_updated_data_source - (count_rows_in_updated_data_source - count_rows_in_db)
                         :]
            list = [
                self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
            ]
            self.get_session().add_all(list)
            self.get_session().commit()
        else:
            self.database.run_migrations()

            list = [
                self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
            ]
            try:
                self.get_session().add_all(list)
                self.get_session().commit()
            # except gevent.Timeout:
            #     sess.invalidate()
            #     raise
            except Exception:
                self.get_session().rollback()
                raise
            return list
        # data_frame = self.data_loader.load_data(data_frame)
        # return data_frame

    def process(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_list = self.load(data_frame)
        return data_list
