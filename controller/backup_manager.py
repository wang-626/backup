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
        user = "-u root"
        password = "-p\"123456\""
        arg = "--skip-add-drop-table --no-create-db --no-create-info --insert-ignore "
        where = f"--where=\"DATE(created_at) <= \'{date}\' AND DATE(updated_at) <= \'{date}\'\" "
        db = "nengren"
        table = "cremation_person cremation_fee cremation_firm cremation_relation cremation_customuser"
        command = f"mysqldump {user} {password} {arg} {where} {db} {table} > {PATH}"

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
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_list = os.listdir(SAVE_PATH)

        if len(file_list) > 0:
            last_date = file_list[-1].split('.')[0]

            while last_date != current_date:
                last_date = datetime.strptime(last_date, "%Y-%m-%d")
                last_date += timedelta(days=1)
                last_date = last_date.strftime("%Y-%m-%d")
                self.backup(last_date)

    def upload_backup(self, date):
        '''upload the backup files by date'''
        self.ssh.connect()
        self.ssh.open_sftp()

        remote_path = f"backups/{date}.sql"
        local_file = os.path.join(SAVE_PATH, f"{date}.sql")

        self.ssh.sftp.put(local_file, remote_path)

        self.ssh.sftp.close()
        self.ssh.disconnect()

    def upload_missing_backup(self):
        '''Compare the local and cloud files, and upload the missing backup files'''
        self.ssh.connect()
        file_list = os.listdir(SAVE_PATH)[-90:]
        remote_path = "backups/"

        self.ssh.open_sftp()
        remote_list = self.ssh.sftp.listdir(remote_path)[-90:]
        lose_list = list(set(file_list) - set(remote_list))

        for file in lose_list:
            local_file = os.path.join(SAVE_PATH, file)
            remote_path = f"backups/{file}"
            self.ssh.sftp.put(local_file, remote_path)

        self.ssh.sftp.close()
        self.ssh.disconnect()

    def insert_log_to_db2(self, date):
        '''insert the log for db2'''
        res = self.db2.query_one(
            f"SELECT * FROM network_disconnection_logs WHERE date = '{date}'")

        if not res:
            values = {'date': date, 'status': 0}
            self.db2.insert_data('network_disconnection_logs', values)
            self.db2.commit_changes()

    def upload_insert_date_to_db2(self, dates):
        '''Insert the missing data for db2'''

        def creat_sql_insert(table, columns):
            ''' create INSERT IGNORE INTO table VALUES '''
            columns_str = ', '.join(columns)
            sql = f'INSERT IGNORE INTO {table}  ({columns_str}) VALUES ({", ".join(["%s"] * len(columns))})'
            return sql

        if self.db1.connect() and self.db2.connect():
            for date in dates:
                for table in dates[date]:
                    columns = self.db1.get_columns_names(table)
                    sql_insert = creat_sql_insert(table, columns)
                    id_list = dates[date][table].split(",")
                    values = self.db1.query_all(
                        f"SELECT * FROM {table} WHERE DATE(created_at) = {date} AND id IN ({','.join(str(id) for id in id_list)})")

                    self.db2.cursor.executemany(sql_insert, list(values))
                    self.db2.commit_changes()
                self.insert_log_to_db2(date)
            self.db1.disconnect()
            self.db2.disconnect()

    def update_date_to_db2(self, dates):
        '''Update the missing data for db2'''

        def creat_sql_update(table, columns):
            ''' create UPDATE table SET columnA = %s, columnB = %s WHERE id = %s AND DATE(updated_at) <= {date}'''
            sql = f'UPDATE {table} SET '
            for i in range(1, len(columns)):
                sql += f"{columns[i]} = %s, "
            sql = sql.rstrip(', ')
            sql += f" WHERE {columns[0]} = %s AND DATE(updated_at) <= {date}"
            return sql

        if self.db1.connect() and self.db2.connect():
            for date in dates:
                for table in dates[date]:
                    columns = self.db1.get_columns_names(table)
                    sql_update = creat_sql_update(table, columns)

                    id_list = dates[date][table].split(",")
                    res = self.db1.query_all(
                        f"SELECT * FROM {table} WHERE DATE(updated_at) = {date} AND id IN ({','.join(id for id in id_list)})")
                    values = list(map(self.format_tuple, list(res)))

                    self.db2.cursor.executemany(sql_update, values)
                    self.db2.commit_changes()
                self.insert_log_to_db2(date)
            self.db1.disconnect()
            self.db2.disconnect()

    def check_backup_logs_list(self):
        '''Checking if table network_disconnection_logs contains any records with a status value of 0'''
        if self.db1.connect():
            res = self.db1.query_one(
                "SELECT date FROM network_disconnection_logs WHERE status = 0")
            self.db1.disconnect()
            return bool(res)
        return False

    def get_backup_logs_list(self):
        ''' format logs_list example:(datetim(2000,1,1))-> ['2000-01-01']'''
        if self.check_backup_logs_list() and self.db1.connect():
            res = self.db1.query_all(
                "SELECT date FROM network_disconnection_logs WHERE status = 0")
            formatted_res = [date[0].strftime("%Y-%m-%d") for date in res]
            self.db1.disconnect()
            return formatted_res
        return None

    def format_tuple(self, value):
        ''' format tuple move the id to the end example:(id,a,b,c)-> (a,b,c,id)'''
        value_list = list(value)
        res = tuple(value_list[1:] + [value_list[0]])
        return res
