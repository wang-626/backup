'''Test controller'''
import unittest
import datetime
import os
from repository import Mysql
from pathlib import Path
from controller import BackupManager, LogChecker, create_log

db_set = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'test'
}

db2_set = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'test2'
}
BASE_DIR = Path(__file__).resolve().parent
DB1 = Mysql(db_set)
DB2 = Mysql(db2_set)
PATH = os.path.join(BASE_DIR, 'fake')
PATH2 = os.path.join(BASE_DIR, 'fake2')


class TestConnectionChecker(unittest.TestCase):
    '''Test ConnectionChecker function'''

    def test_check_log_files(self):
        '''Test check_log_files'''
        checker = LogChecker(PATH)
        res = checker.check_log_files()
        self.assertEqual(res, True)
        checker = LogChecker(PATH2)
        res = checker.check_log_files()
        self.assertEqual(res, False)

    def test_remove_logs(self):
        '''Test log update to db table network_disconnection_logs'''
        with open(os.path.join(PATH2, 'test.log'), 'w'):
            pass
        with open(os.path.join(PATH2, 'test2.log'), 'w'):
            pass
        checker = LogChecker(PATH2)
        checker.remove_logs()
        res = checker.check_log_files()
        self.assertEqual(res, False)


class TestBackupManager(unittest.TestCase):
    '''Test BackupManager function'''

    def test_upload_insert_date_to_db(self):
        '''Test log update to db table network_disconnection_logs'''
        checker = LogChecker(PATH)
        backup = BackupManager(DB1, DB2)
        ids = [1000,1001,1002,1003,1004,1005,1006,1007,1008,1009,1010]
        DB1.connect()
        DB2.connect()
        for id in ids:
            if DB1.query_one(f"SELECT * FROM cremation_relation WHERE id = {id}"):
                DB1.sql(f"DELETE FROM cremation_relation WHERE id = {id}")
                DB1.commit_changes()
            if DB2.query_one(f"SELECT * FROM cremation_relation WHERE id = {id}"):
                DB2.sql(f"DELETE FROM cremation_relation WHERE id = {id}")
                DB2.commit_changes()
            DB1.sql(f"INSERT INTO cremation_relation VALUES({id}, 'test', 'test', 0, '2023-07-03', '2023-07-03')") # 建立新資料測試
            DB1.commit_changes()
        DB1.disconnect()
        DB2.disconnect()
        backup.upload_insert_date_to_db(checker.get_log_files())
        DB1.connect()
        DB2.connect()
        for id in ids:
            res1 = DB1.query_one(f"SELECT * FROM cremation_relation WHERE id = {id}") 
            res2 = DB2.query_one(f"SELECT * FROM cremation_relation WHERE id = {id}")
            self.assertEqual(res1, res2)
        DB1.disconnect()
        DB2.disconnect()

    def test_upload_update_date_to_db(self):
        '''Test log update to db table network_disconnection_logs'''
        checker = LogChecker(PATH)
        backup = BackupManager(DB1, DB2)
        dates = {
            '20230703':{
                'cremation_relation':'1'
                }
            }
        DB1.connect()
        DB2.connect()
        DB1.sql(f"UPDATE cremation_relation SET title='aaaaa', updated_at = '{datetime.date(2023, 7, 3)}' WHERE id = 1") # 建立不同資料測試
        DB2.sql(f"UPDATE cremation_relation SET title='xxxxx', updated_at = '{datetime.date(2023, 7, 3)}' WHERE id = 1")
        DB1.commit_changes()
        DB2.commit_changes()
        DB1.disconnect()
        DB2.disconnect()
        backup.upload_update_date_to_db(dates)
        DB1.connect()
        DB2.connect()
        res1 = DB1.query_one("SELECT title FROM cremation_relation WHERE id = 1") 
        res2 = DB2.query_one("SELECT title FROM cremation_relation WHERE id = 1")
        DB1.disconnect()
        DB2.disconnect()
        self.assertEqual(res1, res2)

    def test_upload_update_date_to_db2(self):
        '''Test 測試當備份資料比現有資料舊時'''
        checker = LogChecker(PATH)
        backup = BackupManager(DB1, DB2)
        dates = {
            '20230703':{
                'cremation_relation':'2'
                }
            }
        DB1.connect()
        DB2.connect()
        DB1.sql(f"UPDATE cremation_relation SET title='aaaaa', updated_at = '{datetime.datetime(2023, 7, 3, 0, 0)}' WHERE id = 2") # 建立不同資料測試
        DB2.sql(f"UPDATE cremation_relation SET title='xxxxx', updated_at = '{datetime.datetime(2023, 7, 5, 0, 0)}' WHERE id = 2")
        DB1.commit_changes()
        DB2.commit_changes()
        DB1.disconnect()
        DB2.disconnect()
        backup.upload_update_date_to_db(dates)
        DB1.connect()
        DB2.connect()
        res1 = DB1.query_one("SELECT updated_at FROM cremation_relation WHERE id = 2") 
        res2 = DB2.query_one("SELECT updated_at FROM cremation_relation WHERE id = 2")
        DB1.disconnect()
        DB2.disconnect()
        self.assertEqual(res1[0], datetime.datetime(2023, 7, 3, 0, 0))
        self.assertEqual(res2[0], datetime.datetime(2023, 7, 5, 0, 0))

    def test_upload_logs_to_db(self):
        '''Test log update to db table network_disconnection_logs'''
        checker = LogChecker(PATH)
        backup = BackupManager(DB1, DB2)
        checker.get_log_files_names()
        DB2.connect()
        backup.upload_logs_to_db('2023-07-03')
        res = DB2.query_one("SELECT * FROM network_disconnection_logs ORDER bY id DESC")
        DB2.disconnect()
        self.assertEqual(res[1], datetime.date(2023, 7, 3))

    def test_check_backup_logs_list(self):
        '''Test check if the backup list has a value '''
        backup = BackupManager(DB1, DB2)
        res1 = backup.check_backup_logs_list()
        DB1.connect()
        res2 = DB1.query_one("SELECT date FROM network_disconnection_logs WHERE status = 0")
        DB1.disconnect()
        if res2:
            self.assertEqual(res1, True)
        else:
            self.assertEqual(res1, False)

    def test_backup(self):
        BASE_DIR = Path(__file__).resolve().parent.parent
        PATH = os.path.join(BASE_DIR, 'backups')
        backup = BackupManager(DB1, DB2)
        backup.backup('2023-07-07')
        file_list = os.listdir(PATH)
        self.assertTrue('2023-07-07.sql' in file_list)
    
    def test_get_backup_logs_list(self):
        backup = BackupManager(DB1, DB2)
        date = '1777-07-07'
        DB1.connect()
        if not DB1.query_one(f"SELECT * FROM network_disconnection_logs WHERE id = 999"):
            values = {
                'id':999,
                'date':date,
                'status':0
            }
            DB1.insert_data("network_disconnection_logs", values)
            DB1.commit_changes()
        else:
            DB1.sql("UPDATE network_disconnection_logs SET status=0 WHERE id = 999")
            DB1.commit_changes()
        DB1.disconnect()
        backup_list = backup.get_backup_logs_list()
        self.assertTrue(date in backup_list)

    def test_backup_by_log(self):
        backup = BackupManager(DB1, DB2)
        ids =[777,778,779,780,781]
        dates = ['2000-01-01','1999-01-01','1998-01-01','1997-01-01','1996-01-01']
        DB1.connect()
        for i, date in enumerate(dates):
            if not DB1.query_one(f"SELECT * FROM network_disconnection_logs WHERE id = {ids[i]}"):
                values = {
                    'id':ids[i],
                    'date':date,
                    'status':0
                }
                DB1.insert_data("network_disconnection_logs", values)
                DB1.commit_changes()
            else:
                DB1.sql(f"UPDATE network_disconnection_logs SET status=0 WHERE id = {ids[i]}")
                DB1.commit_changes()
        DB1.disconnect()
        backup.backup_by_log()
        BASE_DIR = Path(__file__).resolve().parent.parent
        PATH = os.path.join(BASE_DIR, 'backups')
        file_list = os.listdir(PATH)
        for date in dates:
            self.assertTrue(f'{date}.sql' in file_list)


class TestCreateLog(unittest.TestCase):

    def test_log(self):
        create_log('test:123')
        BASE_DIR = Path(__file__).resolve().parent.parent
        PATH = os.path.join(BASE_DIR, 'logs')
        file_list = os.listdir(PATH)
        self.assertTrue(len(file_list)>0)


if __name__ == '__main__':
    unittest.main()
