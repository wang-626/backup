'''backup manager'''
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent
SAVE_PATH = os.path.join(BASE_DIR, 'backups')


class BackupManager:
    '''db1 for local, db2 for syncdb, ssh for upload backup'''

    def __init__(self, db1, db2=None, ssh=None):
        self.db1 = db1
        self.db2 = db2
        self.ssh = ssh

    def backup(self, date):
        '''backup by date using mysqldump'''
        PATH = os.path.join(SAVE_PATH, f'{date}.sql')
        user = '-u root'
        password = '-p\"123456\"'
        arg = '--skip-add-drop-table --no-create-db --no-create-info --insert-ignore '
        where = f'--where=\"DATE(created_at) <= \'{date}\' AND DATE(updated_at) <= \'{date}\'\" '
        db = 'nengren'
        table = 'cremation_person cremation_fee cremation_firm cremation_relation cremation_customuser'
        command = f'mysqldump {user} {password} {arg} {where} {db} {table} > {PATH}'

        result = subprocess.run(command, shell=True, check=True)
        if result.returncode == 0:
            self.upload_backup(date)
        return bool(result.returncode)

    def backup_by_log(self):
        '''read network_disconnection_logs log to rebackup'''
        if self.check_backup_logs_list():
            logs_list = self.get_backup_logs_list()
            self.db1.connect()

            for date in logs_list:
                self.backup(date)

            self.db1.sql(
                f'UPDATE network_disconnection_logs SET status=1 WHERE date in ({", ".join([repr(item) for item in logs_list])})')
            self.db1.commit_changes()
            self.db1.disconnect()

    def backup_by_missing_dates(self):
        '''Compare today and local last backup time to backup'''
        current_date = datetime.now().strftime('%Y-%m-%d')
        file_list = os.listdir(SAVE_PATH)

        if len(file_list) > 0:
            last_date = file_list[-1].split('.')[0]

            while last_date != current_date:
                last_date = datetime.strptime(last_date, '%Y-%m-%d')
                last_date += timedelta(days=1)
                last_date = last_date.strftime('%Y-%m-%d')
                self.backup(last_date)

    def upload_backup(self, date):
        '''upload the backup files by date'''
        self.ssh.connect()
        self.ssh.open_sftp()

        remote_path = f'backups/{date}.sql'
        local_file = os.path.join(SAVE_PATH, f'{date}.sql')

        self.ssh.sftp.put(local_file, remote_path)

        self.ssh.sftp.close()
        self.ssh.disconnect()

    def upload_missing_backup(self):
        '''Compare the local and cloud files, and upload the missing backup files'''
        self.ssh.connect()
        file_list = os.listdir(SAVE_PATH)[-90:]
        remote_path = 'backups/'

        self.ssh.open_sftp()
        remote_list = self.ssh.sftp.listdir(remote_path)[-90:]
        lose_list = list(set(file_list) - set(remote_list))

        for file in lose_list:
            local_file = os.path.join(SAVE_PATH, file)
            remote_path = f'backups/{file}'
            self.ssh.sftp.put(local_file, remote_path)

        self.ssh.sftp.close()
        self.ssh.disconnect()

    def check_backup_logs_list(self):
        '''Checking if table network_disconnection_logs contains any records with a status value of 0'''
        if self.db1.connect():
            res = self.db1.query_one(
                'SELECT date FROM network_disconnection_logs WHERE status = 0')
            self.db1.disconnect()
            return bool(res)
        return False

    def get_backup_logs_list(self):
        ''' format logs_list example:(datetim(2000,1,1))-> ['2000-01-01']'''
        if self.check_backup_logs_list() and self.db1.connect():
            res = self.db1.query_all(
                'SELECT date FROM network_disconnection_logs WHERE status = 0')
            formatted_res = [date[0].strftime('%Y-%m-%d') for date in res]
            self.db1.disconnect()
            return formatted_res
        return None
