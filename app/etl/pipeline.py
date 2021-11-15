from datetime import date, datetime, timedelta
import os
from pytz import timezone

import pandas as pd

from app.etl.transformer import Transformer
from app.exceptions import IncorrectDataSourcePath
from app.models.base import Base
from app.models.metadata import ETL_Metadata
from app.utils import db_session


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
                session.execute(
                    (
                        ('''TRUNCATE TABLE {table}''').format(table=(self.data_class).__tablename__)
                    ) if (os.environ.get('DATABASE_URL')) else (
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
            session.bulk_save_objects(list)
            session.commit()
            session.close()
        return list

    def process(self):
        data_frame = self.extract()
        data_frame = self.transform(data_frame)
        data_frame = self.handle_metadata(data_frame)
        data_list = self.load(data_frame)
        return data_list

