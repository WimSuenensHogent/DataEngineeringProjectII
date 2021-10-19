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
            print(">>> exists")
            Session = sessionmaker(self.database.engine, expire_on_commit=False, autocommit=False)
            session = Session()
            #if we dont want data to fully load again in the local DB,
            #better to compare first and only add a new chunk
            #dropping the existins size of new table
            count_rows_in_db = session.query(self.data_class.id).count()
            print(f"Rows in current db: {count_rows_in_db}")
            count_rows_in_updated_data_source = len(data_frame.index)
            print(f"Rows in current db: {count_rows_in_updated_data_source}")
            diff_old_new = count_rows_in_updated_data_source - (count_rows_in_updated_data_source - count_rows_in_db)
            data_frame = data_frame.iloc[diff_old_new:]
            print(data_frame)
            list = [
                self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
            ]
            session.add_all(list)
            session.commit()
        else:
            self.database.run_migrations()

            list = [
                self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
            ]
            try:
                Session = sessionmaker(self.database.engine, expire_on_commit=False, autocommit=False)
                session = Session()
                session.add_all(list)
                session.commit()
            # except gevent.Timeout:
            #     sess.invalidate()
            #     raise
            except Exception:
                session.rollback()
                raise
            return list
        # data_frame = self.data_loader.load_data(data_frame)
        # return data_frame

    def process(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_list = self.load(data_frame)
        return data_list
