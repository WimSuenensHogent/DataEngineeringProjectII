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
      # data_frame = pipeline.extract()
      # data_frame = pipeline.transform(data_frame)
      # data_frame = pipeline.handle_metadata(data_frame)
      data_list = pipeline.process()
      print(data_list)
