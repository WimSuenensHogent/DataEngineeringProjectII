from datetime import date, datetime, timedelta
import os
from pytz import timezone

import pandas as pd

from app.etl.transformer import Transformer
from app.exceptions import IncorrectDataSourcePath
from app.models.base import Base
from app.models.metadata import ETL_Metadata
from app.utils import db_session, get_db_type


class Pipeline:
    def __init__(
        self,
        data_class: Base,
        path: str,
        transformer: Transformer,
        metadata_handler=None
    ):
        self.data_class = data_class
        self.path = path
        self.transformer = transformer
        self.metadata_handler = metadata_handler
    
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
                if(frequency == "daily"):
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
                date_until_to_filter = datetime.now(timezone('Europe/Brussels')).date()
                # used to reduce the size of the data_frame. Bulk inserts are taking long...
                # min_date_in_data_frame = (data_frame[self.metadata_handler["date_column"]]).min()
                # if (self.last_date_processed == date.min):
                #     self.last_date_processed = (min_date_in_data_frame - timedelta(days=1))
                # max_timedelta_in_days = 90
                # if ((date_until_to_filter - self.last_date_processed).days > max_timedelta_in_days):
                #     date_until_to_filter = self.last_date_processed + timedelta(days=max_timedelta_in_days)
                data_frame = data_frame[
                    (data_frame[self.metadata_handler["date_column"]] > self.last_date_processed) &
                    (data_frame[self.metadata_handler["date_column"]] <= date_until_to_filter)
                    # used for testing...
                    # (data_frame[self.metadata_handler["date_column"]] <= ((datetime.now(timezone('Europe/Brussels')).date()) - timedelta(days=(days_to_extract-3))))
                ]
            
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        if (data_frame.empty):
            return []
        data_list = [
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
                session.execute(
                    (
                        ('''TRUNCATE TABLE {table}''').format(table=(self.data_class).__tablename__)
                    ) if (get_db_type() == "mssql") else (
                        ('''DELETE FROM {table}''').format(table=(self.data_class).__tablename__)
                    )
                )
            etl_metadata = session.query(ETL_Metadata).filter((ETL_Metadata.table == (self.data_class).__tablename__)).first()
            if (etl_metadata):
                etl_metadata.last_date_processed = last_date_processed
            else:
                etl_metadata = ETL_Metadata(
                    (self.data_class).__tablename__,
                    last_date_processed
                )
                session.add(etl_metadata)
            # TODO : Handle by logger
            print("pipeline '{table}' : {length} lines to be added to the database. etl_metadata.last_date_processed will be set to {last_update}".format(
                table=self.data_class.__tablename__,
                length=len(data_list),
                last_update=etl_metadata.last_date_processed
            ))

            # engine = session.get_bind()
            # data_frame.to_sql(
            #     (self.data_class).__tablename__,
            #     con=engine,
            #     if_exists="append",
            #     chunksize=1000,
            #     index=False
            # )
            # lst = [50, 70, 30, 20, 90, 10, 50]
            length = len(data_list)
            step=10000
            array_to_process=[]
            processed=0
            index=0
            while(index < length):
                till_index = index + step
                if (till_index > length):
                    till_index = length
                array_to_process.append(data_list[index:till_index])
                index = till_index

            processed=0
            for i in array_to_process:
                session.bulk_save_objects(i)
                processed = processed + len(i)
                # TODO : Handle by logger
                print("pipeline '{table}' : {processed} of {length} lines added to the database...".format(
                    table=self.data_class.__tablename__,
                    processed=processed,
                    length=len(data_list),
                ))
            # session.bulk_save_objects(data_list)
            session.commit()
            session.close()
        return data_list

    def process(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_frame = self.handle_metadata(data_frame)
        data_list = self.load(data_frame)
        return data_list


