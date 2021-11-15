import json
import os

from app.etl.pipeline import Pipeline
from app.etl.transformer import Transformer
from app.models import models
from app.settings import BASE_DIR

def run():
  with open(
    os.path.join(BASE_DIR, "app/data.json"), mode="r", encoding="UTF-8"
    # os.path.join(BASE_DIR, "app/data_test.json"), mode="r", encoding="UTF-8"
  ) as file:
    data = json.load(file)
    for pl in data['pipelines']:
      try:
        data_class=getattr(models, pl["model"])
        metadata_handler=None
        if "metadata_handler" in dict.keys(pl):
          metadata_handler = pl["metadata_handler"]
        pipeline = Pipeline(
          data_class,
          path=pl["source"],
          transformer=Transformer(pl["tranforms"]),
          metadata_handler=metadata_handler
        )
        data_list = pipeline.process()
        # TODO: Handle by logger
        print("Finished pipeline '{table}' : {length} lines added to the database".format(
          table=data_class.__tablename__,
          length=len(data_list)
        ))
      except:
        # TODO: Handle by logger
        print("Something went wrong...")
        print(pl)
