''' python3.9'''
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from repository import Mysql, Ssh
from controller import BackupManager, SyncManager, LogChecker, create_log


load_dotenv()
db1_set = {
    'host': os.getenv("db1_host"),
    'port': int(os.getenv("db1_port")),
    'user': os.getenv("db1_user"),
    'password': os.getenv("db1_password"),
    'database': os.getenv("db1_database")
}

db2_set = {
    'host': os.getenv("db2_host"),
    'port': int(os.getenv("db2_port")),
    'user': os.getenv("db2_user"),
    'password': os.getenv("db2_password"),
    'database': os.getenv("db2_database"),
}

ssh_set = {
    'hostname': os.getenv("ssh_host"),
    'port': int(os.getenv("ssh_port")),
    'username': os.getenv("ssh_user"),
    'password': os.getenv("ssh_password")
}

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_PATH = os.path.join(BASE_DIR, 'django', 'nengren',
                        'NetworkDisconnectionLogs')
LOG_INSERT_PATH = os.path.join(LOG_PATH, 'insert')
LOG_UPDATE_PATH = os.path.join(LOG_PATH, 'update')


def sync():
    '''同步先檢查insert在檢查update'''
    try:
        DB1 = Mysql(db1_set)
        DB2 = Mysql(db2_set)

        if DB1.connect(test=True) and DB2.connect(test=True):
            checker = LogChecker(LOG_INSERT_PATH)
            sync = SyncManager(DB1, DB2)

            if checker.check_log_files():
                files = checker.get_log_files()
                sync.insert_date_to_db2(files)
                checker.remove_logs()

            checker = LogChecker(LOG_UPDATE_PATH)
            sync = SyncManager(DB1, DB2)

            if checker.check_log_files():
                files = checker.get_log_files()
                sync.update_date_to_db2(files)
                checker.remove_logs()

    except Exception as err:
        create_log(f'sync err {err}')


def backup():
    '''備份順序 之前關機遺漏的->依照資料庫log重新備份->備份今天->補雲端資料庫備份'''
    try:
        DB1 = Mysql(db1_set)
        SSH = Ssh(ssh_set)

        if DB1.connect(test=True):
            current_date = datetime.now().strftime("%Y-%m-%d")
            backup = BackupManager(db1=DB1, ssh=SSH)
            backup.backup_by_missing_dates()  # 補關機時的備份
            backup.backup_by_log()  # 依照資料庫network_disconnection_logs重新備份
            backup.backup(current_date)  # 備份今天
            backup.upload_missing_backup()  # 補雲端資料庫備份

    except Exception as err:
        create_log(f'backup err {err}')


if __name__ == '__main__':
    args = sys.argv
    if sys.argv[1] == 'sync':
        sync()
    if sys.argv[1] == 'backup':
        backup()
