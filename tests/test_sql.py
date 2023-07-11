'''Test sql connection'''
import unittest
from repository.mysql import Mysql



class TestSql(unittest.TestCase):
    '''Test sql function'''

    def test_mysql_connect(self):
        '''Test connect'''
        db_set = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '123456',
            'database': 'nengren'
        }
        db = Mysql(db_set)
        res = db.connect()
        self.assertEqual(res, True)  # sql連線成功
        db_set = {
            'host': 'localhost',
            'port': 3306,
            'user': '1234',
            'password': '123456',
            'database': 'nrngren'
        }
        db = Mysql(db_set)
        res = db.connect()
        self.assertEqual(res, False)  # sql連線失敗


if __name__ == '__main__':
    unittest.main()
