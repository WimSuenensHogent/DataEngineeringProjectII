from __future__ import annotations

import os
import unittest
import json

from app.etl.transformer import Transformer
from app.settings import BASE_DIR
from tests.dummy_data_loader import DummyDataLoader


class TestTransformer(unittest.TestCase):

    def setUp(self):
        self.df = DummyDataLoader().load("vaccinaties.csv")
        self.df1 = DummyDataLoader().load("covid19vac.csv")
        with open(
                os.path.join(BASE_DIR, "tests/dummydata/dummy.json"), mode="r", encoding="UTF-8"
        ) as file:
            self.data = json.load(file)
        with open(
                os.path.join(BASE_DIR, "tests/dummydata/dummy_extra.json"), mode="r", encoding="UTF-8"
        ) as file1:
            self.data1 = json.load(file1)

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

    def test_add_column_returns_data_frame_with_added_column(self):
        pipeline = self.data1['pipeline'][0]
        cols_to_update = pipeline["tranforms"][1]["data"]
        upd_df = Transformer.add_column(self, self.df1, cols_to_update)
        self.assertTrue(10, len(upd_df.columns))

    def test_group_by_returns_sum_population_of_column(self):
        pass


    def test_split_columns_splits_given_column_by_delimiter_into_new_column(self):
        pass



if __name__ == '__main__':
    unittest.main()