from __future__ import annotations

import unittest
import datetime

from app.models.models import CovidConfirmedCasesByCategory


class TestCovidConfirmedCasesByCategory(unittest.TestCase):

    def setUp(self):
        self.row1 = CovidConfirmedCasesByCategory()
        self.row1.date = datetime.date(2021, 1, 1)
        self.row1.province = "West-Vlaanderen"
        self.row1.region = "Vlaanderen"
        self.row1.agegroup = "30-40"
        self.row1.sex = "M"
        self.row1.cases = 100

        self.row1 = CovidConfirmedCasesByCategory()
        self.row1.date = datetime.date(2021, 1, 1)
        self.row1.province = "West-Vlaanderen"
        self.row1.region = "Vlaanderen"
        self.row1.agegroup = "65-70"
        self.row1.sex = "F"
        self.row1.cases = 1000

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_covid_confirmed_cases_by_category")


if __name__ == '__main__':
    unittest.main()