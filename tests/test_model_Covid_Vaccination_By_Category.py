from __future__ import annotations
import unittest
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.models import CovidVaccinationByCategory


class TestCovidVaccinationByCategory(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.session = sessionmaker(bind=self.engine)()
        # CovidVaccinationByCategory.metadata.create_all(self.engine)

        self.row1 = CovidVaccinationByCategory()
        self.row1.date = datetime.date(2021, 2, 2)
        self.row1.region = "Vlaanderen"
        self.row1.agegroup = "0-14"
        self.row1.sex = "M"
        self.row1.brand = "Pfizer"
        self.row1.dose = "1"
        self.row1.count = 100

        self.row2 = CovidVaccinationByCategory()
        self.row2.date = datetime.date(2021, 2, 2)
        self.row2.region = "Vlaanderen"
        self.row2.agegroup = "65+"
        self.row2.sex = "F"
        self.row2.brand = "Moderna"
        self.row2.dose = "2"
        self.row2.count = 1000

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_covid_vaccinations_by_category")


if __name__ == '__main__':
    unittest.main()
