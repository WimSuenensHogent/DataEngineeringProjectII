from __future__ import annotations

import unittest
import datetime

from app.models.models import CovidConfirmedCasesByCategory, DemographicsByNISCodeAndCategory


class TestDemographicsByNISCodeAndCategory(unittest.TestCase):

    def setUp(self):
        self.row1 = DemographicsByNISCodeAndCategory()
        self.row1.year = 2020
        self.row1.nis = "12345"
        self.row1.sex = "F"
        self.row1.nationality_code = "nat_code"
        self.row1.nationality_text_nl = "nat_code_nl"
        self.row1.nationality_text_fr = "nat_code_fr"
        self.row1.marital_status_code = "stat_code"
        self.row1.marital_status_text_nl = "stat_code_nl"
        self.row1.marital_status_text_fr = "stat_code_fr"
        self.row1.age = 50
        self.row1.population = 10000

        self.row2 = DemographicsByNISCodeAndCategory()
        self.row2.year = 2020
        self.row2.nis = "12345"
        self.row2.sex = "F"
        self.row2.nationality_code = "nat_code"
        self.row2.nationality_text_nl = "nat_code_nl"
        self.row2.nationality_text_fr = "nat_code_fr"
        self.row2.marital_status_code = "stat_code"
        self.row2.marital_status_text_nl = "stat_code_nl"
        self.row2.marital_status_text_fr = "stat_code_fr"
        self.row2.age = 50
        self.row2.population = 10000

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_demographics_by_nis_code_and_category")

    def test_nis_code_throws_error_when_len_is_not_five(self):
        with self.assertRaises(ValueError):
            self.row1.validate_nis(0, "123456")

    def test_correct_name_len_of_nis_code(self):
        self.assertEqual(len(self.row1.nis), 5)

if __name__ == '__main__':
    unittest.main()