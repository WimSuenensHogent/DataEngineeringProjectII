from __future__ import annotations

import unittest
import datetime

from app.models.models import CovidConfirmedCasesByCategory, DemographicsByNISCodeAndCategory, \
    VaccinationsByNISCodeDailyUpdated


class TestVaccinationsByNISCodeDailyUpdated(unittest.TestCase):

    def setUp(self):
        self.row1 = VaccinationsByNISCodeDailyUpdated()
        self.row1.nis_code = "12345"
        self.row1.sex = "F"
        self.row1.agegroup = "10-19"
        self.row1.plus18 = True
        self.row1.plus65 = False
        self.row1.vaccinated_fully_total = 50
        self.row1.vaccinated_partly_total = 20
        self.row1.vaccinated_fully_astrazeneca = 10
        self.row1.vaccinated_partly_astrazeneca = 5
        self.row1.vaccinated_fully_pfizer = 10
        self.row1.vaccinated_partly_pfizer = 5
        self.row1.vaccinated_fully_moderna = 10
        self.row1.vaccinated_partly_moderna = 5
        self.row1.vaccinated_fully_johnsonandjohnson = 10
        self.row1.vaccinated_fully_other = 10
        self.row1.vaccinated_partly_other = 5
        self.row1.population_by_agecategory_and_municipality = 100000

        self.row2 = VaccinationsByNISCodeDailyUpdated()
        self.row2.nis_code = "54321"
        self.row2.sex = "M"
        self.row2.agegroup = "10-19"
        self.row2.plus18 = True
        self.row2.plus65 = False
        self.row2.vaccinated_fully_total = 50
        self.row2.vaccinated_partly_total = 20
        self.row2.vaccinated_fully_astrazeneca = 10
        self.row2.vaccinated_partly_astrazeneca = 5
        self.row2.vaccinated_fully_pfizer = 10
        self.row2.vaccinated_partly_pfizer = 5
        self.row2.vaccinated_fully_moderna = 10
        self.row2.vaccinated_partly_moderna = 5
        self.row2.vaccinated_fully_johnsonandjohnson = 10
        self.row2.vaccinated_fully_other = 10
        self.row2.vaccinated_partly_other = 5
        self.row2.population_by_agecategory_and_municipality = 100000

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_vaccinations_by_nis_code_daily_updated")

    def test_nis_code_throws_error_when_len_is_not_five(self):
        with self.assertRaises(ValueError):
            self.row1.validate_nis_code(0, "123456")

    def test_correct_name_len_of_nis_code(self):
        self.assertEqual(len(self.row1.nis_code), 5)

if __name__ == '__main__':
    unittest.main()