from __future__ import annotations
import unittest
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.models import NumberOfDeathsByDistrictNISCode

class TestNumberOfDeathsByDistrictNISCode(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        self.session = sessionmaker(bind=self.engine)()
        NumberOfDeathsByDistrictNISCode.metadata.create_all(self.engine)

        self.row1 = NumberOfDeathsByDistrictNISCode()
        self.row1.date = datetime.date(2021, 2, 2)
        self.row1.nis_district = "12345"
        self.row1.sex = "F"
        self.row1.agegroup = "65+"
        self.row1.number_of_deaths = 100

        self.row2 = NumberOfDeathsByDistrictNISCode()
        self.row2.date = datetime.date(2021, 3, 3)
        self.row2.nis_district = "54321"
        self.row2.sex = "M"
        self.row2.agegroup = "65+"
        self.row2.number_of_deaths = 10

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "fact_number_of_deaths_by_district_nis_code")

    def test_nis_code_throws_error_when_len_is_not_five(self):
        with self.assertRaises(ValueError):
            self.row1.validate_nis_district(0, "123456")

    def test_correct_name_len_of_nis_code(self):
        self.assertEqual(len(self.row1.nis_district), 5)

if __name__ == '__main__':
    unittest.main()