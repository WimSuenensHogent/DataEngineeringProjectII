import pandas as pd
from sqlalchemy.orm import sessionmaker
from app.models.models import Base
from app.etl.transformer import Transformer

class Pipeline():
    def __init__(self, database, data_class: Base, csv_path: str, transformer: Transformer):
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
        if self.transformer :
            data_frame = self.transformer.transform_data_frame(data_frame)        
        return data_frame


    def load(self, data_frame: pd.DataFrame):
        list = [self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient='records')]
        try:
            Session = sessionmaker(self.database.engine, expire_on_commit=False)
            session = Session()
            session.add_all(list)
            session.commit()
        # except gevent.Timeout:
        #     sess.invalidate()
        #     raise
        except:
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
