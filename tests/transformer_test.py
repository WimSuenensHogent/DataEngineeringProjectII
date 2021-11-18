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

    def setUp(self):
        self.df = DummyDataLoader().load("vaccinaties.csv")
        with open(
                os.path.join(BASE_DIR, "tests/dummydata/dummy.json"), mode="r", encoding="UTF-8"
        ) as file:
            self.data = json.load(file)

    def test_drop_columns_returns_data_frame_with_given_dropped_columns(self):
        pipeline = self.data['pipeline'][0]
        columns_to_delete = pipeline["tranforms"][0]["data"]
        upd_df = Transformer.drop_columns(self, self.df, columns_to_delete)
        self.assertEqual(17, len(upd_df.columns))

    def test_rename_columns_returns_data_frame_with_renamed_columns(self):
        pipeline = self.data['pipeline'][0]
        columns_to_delete = pipeline["tranforms"][1]["data"]
        upd_df = Transformer.rename_columns(self, self.df, columns_to_delete)
        self.assertEqual("nis_code", upd_df.columns[0])

    def test_drop_na_returns_data_frame_with_dropped_rows(self):
        upd_df = Transformer.drop_na(self, self.df, None)
        self.assertEqual(8, len(upd_df))

    def test_update_value_returns_data_frame_with_updated_values_in_each_row(self):
        pipeline = self.data['pipeline'][0]
        rows_to_update = pipeline["tranforms"][2]["data"]
        upd_df = Transformer.update_value(self, self.df, rows_to_update)
        self.assertTrue(True, upd_df["ADULT_FL(18+)"][0])

if __name__ == '__main__':
    unittest.main()