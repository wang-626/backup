class SyncManager:

    def __init__(self, db1, db2=None, ssh=None):
        self.db1 = db1
        self.db2 = db2

    def insert_log_to_db2(self, date):
        '''insert the log for db2'''
        res = self.db2.query_one(
            f'SELECT * FROM network_disconnection_logs WHERE date = \'{date}\'')

        if not res:
            values = {'date': date, 'status': 0}
            self.db2.insert_data('network_disconnection_logs', values)
            self.db2.commit_changes()

    def insert_date_to_db2(self, files):
        '''Insert the missing data for db2'''

        def creat_sql_insert(table, columns):
            ''' create INSERT IGNORE INTO table VALUES '''
            columns_str = ', '.join(columns)
            sql = f'INSERT IGNORE INTO {table}  ({columns_str}) VALUES ({", ".join(["%s"] * len(columns))})'
            return sql

        if self.db1.connect() and self.db2.connect():
            for file in files:
                for table in files[file]:
                    date = file.split('.')[0]
                    columns = self.db1.get_columns_names(table)
                    sql_insert = creat_sql_insert(table, columns)
                    id_list = files[file][table].split(':')
                    values = self.db1.query_all(
                        f'SELECT * FROM {table} WHERE DATE(created_at) = "{date}" AND id IN ({",".join(str(id) for id in id_list)})')

                    self.db2.cursor.executemany(sql_insert, list(values))
                    self.db2.commit_changes()
                self.insert_log_to_db2(date)
        self.db1.disconnect()
        self.db2.disconnect()

    def update_date_to_db2(self, files):
        '''Update the missing data for db2'''

        def creat_sql_update(table, columns, date):
            ''' create UPDATE table SET columnA = %s, columnB = %s WHERE id = %s AND DATE(updated_at) <= {date}'''
            sql = f'UPDATE {table} SET '
            for i in range(1, len(columns)):
                sql += f'{columns[i]} = %s, '
            sql = sql.rstrip(', ')
            sql += f' WHERE {columns[0]} = %s AND DATE(updated_at) <= "{date}"'
            return sql

        if self.db1.connect() and self.db2.connect():
            for file in files:
                for table in files[file]:
                    date = file.split('.')[0]
                    columns = self.db1.get_columns_names(table)
                    sql_update = creat_sql_update(table, columns, date)

                    id_list = files[date][table].split(':')
                    res = self.db1.query_all(
                        f'SELECT * FROM {table} WHERE DATE(updated_at) = "{date}" AND id IN ({",".join(id for id in id_list)})')
                    values = list(map(self.format_tuple, list(res)))
                    self.db2.cursor.executemany(sql_update, values)
                    self.db2.commit_changes()
                self.insert_log_to_db2(date)
        self.db1.disconnect()
        self.db2.disconnect()

    def format_tuple(self, value):
        ''' format tuple move the id to the end example:(id,a,b,c)-> (a,b,c,id)'''
        value_list = list(value)
        res = tuple(value_list[1:] + [value_list[0]])
        return res