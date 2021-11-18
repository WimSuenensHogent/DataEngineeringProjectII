from __future__ import annotations

import sqlite3
import unittest
import datetime

import mock
import pytest

from app.models.models import NIS_Code, CovidMortalityByCategory
from unittest.mock import MagicMock


class TestCovidMortalityByCategory(unittest.TestCase):

    def setUp(self):
        self.row1 = CovidMortalityByCategory()
        self.row1.date = datetime.date(2021, 1, 1)
        self.row1.region = "Vlaanderen"
        self.row1.agegroup = "30-40"
        self.row1.sex = "F"
        self.row1.deaths = 100

        self.row2 = CovidMortalityByCategory()
        self.row2.date = datetime.date(2021, 1, 1)
        self.row2.region = "Ost-Belgien"
        self.row2.agegroup = "20-29"
        self.row2.sex = "M"
        self.row2.deaths = 10

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_covid_mortality_by_category")


if __name__ == '__main__':
    unittest.main()