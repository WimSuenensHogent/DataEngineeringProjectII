import datetime
from datetime import date

import pandas as pd
from sqlalchemy import inspect

from sqlalchemy.orm import Session

from app.etl.transformer import CommonTransformer
from app.exceptions import IncorrectDataSourcePath
from app.models.models import Base, LastPipeLineProcessing
from app.utils import get_db_engine


class Pipeline:
    def __init__(
        self,
        data_class: Base,
        path: str,
        transformer: CommonTransformer,
        isLast: bool
    ):
        self.data_class = data_class
        self.path = path
        self.transformer = transformer
        self.isLast = isLast
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

        #Voor testen laat ik da voorlopig staan, dat de tabellen vanuit echte data source raper worden geladen
        #Kan verdee gemakelijk aangeapst worden
        if not inspect(get_db_engine()).has_table(self.data_class.__tablename__): #Wordt via alembic gedaan.
            print("Table does not exists, add table first")
            self.add_all_and_commit(data_frame, session)
        else: #als tabellen verschillen van lengte.
            print(f"Table exists, checking changes for table {self.data_class.__tablename__} ...")
            self.compare_outsourced_and_local_db_and_append_if_changed(data_frame, session)


    def process(self, session: Session):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_list = self.load(session, data_frame)
        return data_list

    def add_all_and_commit(self, data_frame: pd.DataFrame, session: Session):
        list = [
            self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
        ]
        # session.add_all(list)
        session.bulk_save_objects(list)
        session.commit()

    def compare_outsourced_and_local_db_and_append_if_changed(self, data_frame: pd.DataFrame, session: Session):
        last_date_processing = session.query(LastPipeLineProcessing).get(1) #last processing of data
        now_date_processing = date.today() # + datetime.timedelta(days=16) #for test
        print(f">>>>>>>>>> Last processing date?: {last_date_processing.date}")
        print(f">>>>>>>>>> Last pipeline?: {self.isLast}")
        if last_date_processing:
            if 'date' in data_frame:
                print(f">>> last_date_processing: {last_date_processing.date}")
                print(f">>> todays date: {now_date_processing}")
                print(f">>> Date in data frame: {data_frame['date'][0]}")
                data_frame = data_frame[~(pd.to_datetime(data_frame['date']) <= pd.to_datetime(last_date_processing.date))] # drop all data below last day when the data was processed
                print(f">>> last_date_processing is updated to: {now_date_processing}")
                self.add_all_and_commit(data_frame, session)  # add full data
            else:
                count_rows_in_db = session.query(self.data_class.id).count()
                print(f"Rows in local table {self.data_class.__tablename__} db: {count_rows_in_db}")
                count_rows_in_updated_data_source = len(data_frame.index)
                print(
                    f"Rows in outsourced table {self.data_class.__tablename__} db: {count_rows_in_updated_data_source}")
                if count_rows_in_updated_data_source > count_rows_in_db:
                    print(">>> Cutting data")
                    data_frame = data_frame.iloc[
                                 count_rows_in_updated_data_source
                                 - (count_rows_in_updated_data_source - count_rows_in_db):
                                 ]
                    self.add_all_and_commit(data_frame, session)  # add full data
        else:
            print(f"Adding all data")
            self.add_all_and_commit(data_frame, session) #add full data
        if self.isLast:
            print(f">>>>>>>>>>>>> now merging date of last processing time")
            session.merge(LastPipeLineProcessing(id=1, date=now_date_processing))  # update last date processing
            # only when processing last pipeline, if next pipeline contains 'date', it will be skipped because of last_processing_date already exists


