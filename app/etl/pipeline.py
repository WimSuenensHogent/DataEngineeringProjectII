from datetime import date, datetime, timedelta
import os
from pytz import timezone

import pandas as pd
from sqlalchemy import inspect

from sqlalchemy.orm import Session

from app.etl.transformer import CommonTransformer, Transformer
from app.exceptions import IncorrectDataSourcePath
from app.models.base import Base
from app.models.metadata import ETL_Metadata
# from app.models.models import Base, LastPipeLineProcessing
from app.utils import db_session, get_db_engine


class Pipeline:
    def __init__(
        self,
        data_class: Base,
        path: str,
        transformer: Transformer or CommonTransformer,
        metadata_handler=None
        # isLast: bool
    ):
        self.data_class = data_class
        self.path = path
        self.transformer = transformer
        self.metadata_handler = metadata_handler
        # self.isLast = isLast
    
    def extract(self):
        """Haalt data op van url

        Returns:
            pd.DataFrame: [description]
        """
        if (self.metadata_handler):
            if("frequency" in dict.keys(self.metadata_handler)):
                frequency = self.metadata_handler["frequency"]
                if(frequency == "yearly"):
                    with db_session(echo=False) as session:
                        etl_metadata = session.query(ETL_Metadata).filter((ETL_Metadata.table == (self.data_class).__tablename__)).first()
                        session.close()
                    if (etl_metadata):
                        if(etl_metadata.last_date_processed >= datetime.now(timezone('Europe/Brussels')).date().replace(month=1, day=1)):
                            return pd.DataFrame()
                if(frequency == "yearly"):
                    if("full_refresh" in dict.keys(self.metadata_handler)):
                        full_refresh = self.metadata_handler["full_refresh"]
                        if (full_refresh):
                            with db_session(echo=False) as session:
                                etl_metadata = session.query(ETL_Metadata).filter((ETL_Metadata.table == (self.data_class).__tablename__)).first()
                                session.close()
                            if (etl_metadata):
                                if(etl_metadata.last_date_processed >= datetime.now(timezone('Europe/Brussels')).date()):
                                    return pd.DataFrame()

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
        if (data_frame.empty):
            return data_frame
        return self.transformer.transform(data_frame)
        # return self.transformer.transform(data_frame, self.path)

    def handle_metadata(self, data_frame: pd.DataFrame):
        if (not self.metadata_handler):
            return data_frame

        if (data_frame.empty):
            return data_frame
        
        self.last_date_processed = date.min
        frequency = 'daily'
        if ("frequency" in dict.keys(self.metadata_handler)):
            frequency = self.metadata_handler["frequency"]
            if (frequency != "daily"):
                self.last_date_processed = datetime.now(timezone('Europe/Brussels')).date()
            if ("full_refresh" in dict.keys(self.metadata_handler)):
                full_refresh = self.metadata_handler["full_refresh"]
                if (full_refresh):
                    self.last_date_processed = datetime.now(timezone('Europe/Brussels')).date()
        # used for testing...
        # days_to_extract = 30
        # today = datetime.now(timezone('Europe/Brussels')).date()
        # self.last_date_processed = today - timedelta(days=days_to_extract)

        with db_session(echo=False) as session:
            etl_metadata = session.query(ETL_Metadata).filter((ETL_Metadata.table == (self.data_class).__tablename__)).first()
            session.close()
        if (etl_metadata):
            self.last_date_processed = etl_metadata.last_date_processed
            # used for testing...
            # days_to_extract = ((datetime.now(timezone('Europe/Brussels')).date()) - self.last_date_processed).days
        if (self.metadata_handler):
            if ("date_column" in dict(self.metadata_handler)):
                data_frame = data_frame[
                    (data_frame[self.metadata_handler["date_column"]] > self.last_date_processed) &
                    (data_frame[self.metadata_handler["date_column"]] <= (datetime.now(timezone('Europe/Brussels')).date()))
                    # used for testing...
                    # (data_frame[self.metadata_handler["date_column"]] <= ((datetime.now(timezone('Europe/Brussels')).date()) - timedelta(days=(days_to_extract-3))))
                ]
            
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        if (data_frame.empty):
            return []
        list = [
            self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
        ]
        last_date_processed = self.last_date_processed

        truncate_table=False
        if (self.metadata_handler):
            if ("date_column" in dict.keys(self.metadata_handler)):
                date_column = self.metadata_handler["date_column"]
                last_date_processed = (data_frame[date_column]).max()
            if ("full_refresh" in dict.keys(self.metadata_handler)):
                full_refresh = self.metadata_handler["full_refresh"]
                if(full_refresh):
                    truncate_table=True
        with db_session() as session:
            if(truncate_table):
                print(('trunctate {table}').format(table=(self.data_class).__tablename__))
                session.execute(
                    (
                        ('''TRUNCATE TABLE {table}''').format(table=(self.data_class).__tablename__)
                    ) if (os.environ.get('DATABASE_URL')) else (
                        ('''DELETE FROM {table}''').format(table=(self.data_class).__tablename__)
                    )
                )
                # session.commit()
            etl_metadata = session.query(ETL_Metadata).filter((ETL_Metadata.table == (self.data_class).__tablename__)).first()
            if (etl_metadata):
                etl_metadata.last_date_processed = last_date_processed
            else:
                etl_metadata = ETL_Metadata(
                    (self.data_class).__tablename__,
                    last_date_processed
                )
                session.add(etl_metadata)
            session.bulk_save_objects(list)
            session.commit()
            session.close()
        return list



        # if we dont want data to fully load again in the local DB,
        # better to compare first and only add a new chunk
        # dropping the existins size of new table

        #Voor testen laat ik da voorlopig staan, dat de tabellen vanuit echte data source raper worden geladen
        #Kan verdee gemakelijk aangeapst worden
        # if not inspect(get_db_engine()).has_table(self.data_class.__tablename__): #Wordt via alembic gedaan.
        #     print("Table does not exists, add table first")
        #     self.add_all_and_commit(data_frame, session)
        # else: #als tabellen verschillen van lengte.
        #     print(f"Table exists, checking changes for table {self.data_class.__tablename__} ...")
        #     self.compare_outsourced_and_local_db_and_append_if_changed(data_frame, session)


    # def process(self, session: Session):
    def process(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_frame = self.handle_metadata(data_frame)
        print(data_frame)
        data_list = self.load(data_frame)
        return data_list

    # def add_all_and_commit(self, data_frame: pd.DataFrame, session: Session):
    #     list = [
    #         self.data_class(**kwargs) for kwargs in data_frame.to_dict(orient="records")
    #     ]
    #     # session.add_all(list)
    #     session.bulk_save_objects(list)
    #     session.commit()

    # def compare_outsourced_and_local_db_and_append_if_changed(self, data_frame: pd.DataFrame, session: Session):
    #     last_date_processing = session.query(LastPipeLineProcessing).get(1) #last processing of data
    #     now_date_processing = date.today() # + timedelta(days=16) #for test
    #     print(f">>>>>>>>>> Last processing date?: {last_date_processing.date}")
    #     # print(f">>>>>>>>>> Last pipeline?: {self.isLast}")
    #     if last_date_processing:
    #         if 'date' in data_frame:
    #             print(f">>> last_date_processing: {last_date_processing.date}")
    #             print(f">>> todays date: {now_date_processing}")
    #             print(f">>> Date in data frame: {data_frame['date'][0]}")
    #             data_frame = data_frame[~(pd.to_datetime(data_frame['date']) <= pd.to_datetime(last_date_processing.date))] # drop all data below last day when the data was processed
    #             print(f">>> last_date_processing is updated to: {now_date_processing}")
    #             self.add_all_and_commit(data_frame, session)  # add full data
    #         else:
    #             count_rows_in_db = session.query(self.data_class.id).count()
    #             print(f"Rows in local table {self.data_class.__tablename__} db: {count_rows_in_db}")
    #             count_rows_in_updated_data_source = len(data_frame.index)
    #             print(
    #                 f"Rows in outsourced table {self.data_class.__tablename__} db: {count_rows_in_updated_data_source}")
    #             if count_rows_in_updated_data_source > count_rows_in_db:
    #                 print(">>> Cutting data")
    #                 data_frame = data_frame.iloc[
    #                              count_rows_in_updated_data_source
    #                              - (count_rows_in_updated_data_source - count_rows_in_db):
    #                              ]
    #                 self.add_all_and_commit(data_frame, session)  # add full data
    #     else:
    #         print(f"Adding all data")
    #         self.add_all_and_commit(data_frame, session) #add full data
    #     # if self.isLast:
    #     #     print(f">>>>>>>>>>>>> now merging date of last processing time")
    #     #     session.merge(LastPipeLineProcessing(id=1, date=now_date_processing))  # update last date processing
    #     #     # only when processing last pipeline, if next pipeline contains 'date', it will be skipped because of last_processing_date already exists


