from MSSQLDB_connect import MSSQLDB
import pandas as pd
from datetime import datetime

class Manager:
    def __init__(self, logger, dbconn_config ,time_start ,time_end):
        self.logger = logger
        self.dbmanager = MSSQLDB(self.logger, dbconn_config)
        self.start = time_start
        self.end = time_end
    
    def get_db_table(self, table_name, creat_dt = None):
        if creat_dt:
            if table_name == 'Behavior':
                date_start = datetime.strptime(self.start, '%Y-%m-%d')
                unix_start = int(date_start.timestamp()*1000)
                date_end = datetime.strptime(self.end, '%Y-%m-%d')
                unix_end = int(date_end.timestamp()*1000)
                sql_cmd = "SELECT * FROM {table_name} WHERE HitTime >= '{start}' AND HitTime < '{end}'".format(table_name=table_name,start = unix_start,end = unix_end)
            else:
                sql_cmd = "SELECT * FROM {table_name} WHERE OrderDateTime >= '{start}' AND OrderDateTime < '{end}'".format(table_name=table_name,start = self.start,end = self.end)
        else:
            sql_cmd = 'SELECT * FROM {table_name}'.format(table_name=table_name)
        table_val, table_col = self.dbmanager.query_database(sql_cmd)
        df = pd.DataFrame(table_val, columns=[val[0] for val in table_col])
        return df