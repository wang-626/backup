import pymysql


class Mysql:

    def __init__(self, db_set):
        self.db_set = db_set
        self.conn = None
        self.cursor = None

    def connect(self, test=False) -> bool:
        '''database connect return true or false'''
        try:
            self.conn = pymysql.connect(**self.db_set)
            self.cursor = self.conn.cursor()
            if test:
                self.disconnect()
            return True
        except Exception as e:
            return False

    def query_one(self, query):
        '''returns a single record or None'''
        try:
            query = query + " LIMIT 1"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result
        except Exception as error:
            print('Error executing query:', error)
            return False

    def query_all(self, query):
        '''returns all the rows or None of a query result'''
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as error:
            print('Error executing query:', error)
            return False
    
    def insert_data(self, table, data) -> None:
        '''
        Database insert value using dictionary format

        example:

        {
          'column1':123,

          'column2':456,
        }
        '''
        columns = ', '.join(data.keys())
        values = ', '.join([f'"{value}"' for value in data.values()])
        data = f'INSERT INTO {table} ({columns}) VALUES ({values})'
        self.cursor.execute(data)

    def get_columns_names(self, table):
        '''return list columns names exampe:['id','date'...]'''
        try:
            columns =  self.query_all(f'SHOW COLUMNS FROM {table}')
            columns = [f'`{column[0]}`' for column in columns]
            return columns
        except:
            return []

    
    def sql(self, sql_string):
        '''use raw sql '''
        self.cursor.execute(sql_string)

    def commit_changes(self):
        '''database commit push'''
        try:
            self.conn.commit()
            return True
        except Exception as error:
            print('Error committing changes:', error)
            self.conn.rollback()
            return False

    def disconnect(self):
        '''database disconnect '''
        if self.cursor and self.cursor.connection:
            self.cursor.close()
        if self.conn and self.conn.open:
            self.conn.close()
    
    def __del__(self):
        '''close sql if sql not close'''
        if self.conn and self.conn.open:
            self.disconnect()
