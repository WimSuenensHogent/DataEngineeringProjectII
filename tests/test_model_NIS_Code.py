from __future__ import annotations

import sqlite3
import unittest
import datetime

import mock
import pytest

from app.models.models import NIS_Code
from unittest.mock import MagicMock, patch


class TestNIS_Code(unittest.TestCase):

    def setUp(self):
        self.row1 = NIS_Code()
        self.row1.nis = "12345"
        self.row1.parent_nis = "54321"
        self.row1.level = 5
        self.row1.text_nl = "Dinant"
        self.row1.text_fr = "Dinan"
        self.row1.text_de = "Dinant"
        self.row1.valid_from = datetime.date(2021, 1, 1)
        self.row1.valid_till = datetime.date(9999, 1, 1)

        self.row1 = NIS_Code()
        self.row1.nis = "23456"
        self.row1.parent_nis = "65432"
        self.row1.level = 10
        self.row1.text_nl = "Luik"
        self.row1.text_fr = "Liège"
        self.row1.text_de = "Lüttich"
        self.row1.valid_from = datetime.date(2021, 2, 2)
        self.row1.valid_till = datetime.date(9999, 2, 2)

    def test_correct_name_of_table(self):
        self.assertEqual(self.row1.__tablename__, "dim_nis_codes")

    def test_nis_code_throws_error_when_len_is_not_five(self):
        with self.assertRaises(ValueError):
            self.row1.validate_nis(0, "123456")

    def test_correct_name_len_of_nis_code(self):
        self.assertEqual(len(self.row1.nis), 5)

    # @pytest.mark.parametrize("val,expected", [(self.row1, self.row2), (row1, row2)])
    # def test_get_all_nis_codes(self, val, expected):
    #     db = NIS_Code()
    #     mock_session = MagicMock()
    #
    #     mock_session.configure_mock(
    #         **{
    #             "get_all_sqlalchemy.return_value": val
    #         }
    #     )
    #     setattr(db, "sqlalchemy_orm", mock_session)
    #     df = db.get_all(expected)
    #     assert df == expected

    # def test_sqlite3_connect_success(self):
    #     sqlite3.connect = MagicMock(return_value='connection succeeded')
    #     dbc = DataBaseClass()
    #     sqlite3.connect.assert_called_with('test_database')
    #     self.assertEqual(dbc.connection, 'connection succeeded')
    #
    # def test_sqlite3_connect_fail(self):
    #     sqlite3.connect = MagicMock(return_value='connection failed')
    #
    #     dbc = DataBaseClass()
    #     sqlite3.connect.assert_called_with('test_database')
    #     self.assertEqual(dbc.connection, 'connection failed')

    @patch('app.etl.pipeline.db_session')
    def test_asd(self, db_session):
        pass


if __name__ == '__main__':
    unittest.main()


# class DataBaseClass():
#     def __init__(self, connection_string='test_database'):
#         self.connection = sqlite3.connect(connection_string)
