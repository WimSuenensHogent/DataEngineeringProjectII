import json
import os
import traceback

from app.etl.pipeline import Pipeline
from app.etl.transformer import Transformer
from app.models import models
from app.settings import BASE_DIR
from app.logger import get_logger

def run():
    logger = get_logger(__name__)

    with open(
        os.path.join(BASE_DIR, "app/data.json"),
        mode="r",
        encoding="UTF-8"
        # os.path.join(BASE_DIR, "app/data_test.json"), mode="r", encoding="UTF-8"
    ) as file:
        data = json.load(file)
        for pl in data["pipelines"][-1:]:
            try:
                data_class = getattr(models, pl["model"])
                metadata_handler = None
                if "metadata_handler" in dict.keys(pl):
                    metadata_handler = pl["metadata_handler"]
                pipeline = Pipeline(
                    data_class,
                    path=pl["source"],
                    transformer=Transformer(pl["tranforms"]),
                    metadata_handler=metadata_handler,
                )
                # data_frame = pipeline.extract()
                # data_frame = pipeline.transform(data_frame)
                # print(data_frame)
                # print(data_frame.describe())
                # print(data_frame.info())
                logger.info("Starting to process pipeline '{table}'...".format(
                    table=data_class.__tablename__
                ))
                data_list = pipeline.process()
                logger.info(
                    "Finished pipeline '{table}' : {length} lines added to the database".format(
                        table=data_class.__tablename__, length=len(data_list)
                    ),
                )
            except Exception as e:
                model="Unknown"
                if (type(pl) == dict):
                    if ("model" in dict.keys(pl)):
                        model = pl["model"] 
                logger.error(
                    "Pipeline '{model}' : {exception}".format(
                        model=model,
                        exception=e
                    ),
                    exc_info=True
                )
            
