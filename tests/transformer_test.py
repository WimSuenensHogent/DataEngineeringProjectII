from __future__ import annotations

import os
import unittest
import json
import datetime

from app.etl.transformer import Transformer
from app.models.models import CovidConfirmedCasesByCategory, DemographicsByNISCodeAndCategory, \
    VaccinationsByNISCodeDailyUpdated
from app.settings import BASE_DIR
from tests.dummy_data_loader import DummyDataLoader


class TestTransformer(unittest.TestCase):

    def test_print(self):
        dummy = DummyDataLoader()
        df = dummy.load("vaccinaties.csv")
        print(len(df.columns))
        with open(
                os.path.join(BASE_DIR, "tests/dummydata/dummy.json"), mode="r", encoding="UTF-8"
        ) as file:
            data = json.load(file)
            pipeline = data['pipeline'][0]
            columns_to_delete = pipeline["tranforms"][0]["data"]
            upd_df = Transformer.drop_columns(self, df, columns_to_delete)
            print(upd_df)
            self.assertEqual(17, len(upd_df.columns))

if __name__ == '__main__':
    unittest.main()