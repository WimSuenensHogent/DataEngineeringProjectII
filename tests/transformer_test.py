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

    def test_drop_columns_returns_data_frame_with_given_dropped_columns(self):
        dummy = DummyDataLoader()
        df = dummy.load("vaccinaties.csv")
        with open(
                os.path.join(BASE_DIR, "tests/dummydata/dummy.json"), mode="r", encoding="UTF-8"
        ) as file:
            data = json.load(file)
            pipeline = data['pipeline'][0]
            columns_to_delete = pipeline["tranforms"][0]["data"]
            upd_df = Transformer.drop_columns(self, df, columns_to_delete)
            self.assertEqual(17, len(upd_df.columns))

    def test_rename_columns_returns_data_frame_with_renamed_columns(self):
        dummy = DummyDataLoader()
        df = dummy.load("vaccinaties.csv")
        with open(
                os.path.join(BASE_DIR, "tests/dummydata/dummy.json"), mode="r", encoding="UTF-8"
        ) as file:
            data = json.load(file)
            pipeline = data['pipeline'][0]
            columns_to_delete = pipeline["tranforms"][1]["data"]
            upd_df = Transformer.rename_columns(self, df, columns_to_delete)
            self.assertEqual("nis_code", upd_df.columns[0])


if __name__ == '__main__':
    unittest.main()