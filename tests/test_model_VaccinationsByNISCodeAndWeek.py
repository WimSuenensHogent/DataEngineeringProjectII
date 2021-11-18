from __future__ import annotations

import unittest
import datetime

from app.models.models import CovidConfirmedCasesByCategory, DemographicsByNISCodeAndCategory, \
    VaccinationsByNISCodeAndWeek


class TestVaccinationsByNISCodeAndWeek(unittest.TestCase):

    def setUp(self):
        self.row1 = VaccinationsByNISCodeAndWeek()
        self.row1.date = datetime.date(2021, 12, 30)
        self.row1.year = 2021
        self.row1.week = 52
        self.row1.nis_code = "12345"
        self.row1.agegroup = "65+"
        self.row1.dose = "1"
        self.row1.cumul_of_week = 40

        self.row2 = VaccinationsByNISCodeAndWeek()
        self.row2.date = datetime.date(2021, 12, 30)
        self.row2.year = 2021
        self.row2.week = 52
        self.row2.nis_code = "54321"
        self.row2.agegroup = "30-39"
        self.row2.dose = "2"
        self.row2.cumul_of_week = 100

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_vaccinations_by_nis_code_and_week")

    def test_nis_code_throws_error_when_len_is_not_five(self):
        with self.assertRaises(ValueError):
            self.row1.validate_nis_code(0, "123456")

    def test_correct_name_len_of_nis_code(self):
        self.assertEqual(len(self.row1.nis_code), 5)

if __name__ == '__main__':
    unittest.main()