import pymssql
import pandas as pd
from dbutils.pooled_db import PooledDB, PooledDBError
from datetime import datetime

class MSSQLDB:
    def __init__(self, logger, config):
        self.db_config = config
        self.logger = logger
    
    def query_database(self, sql):
        try:
            self.pool = PooledDB(**self.db_config)
            conn = self.pool.connection()
            cursor_obj = conn.cursor()
            cursor_obj.execute(sql)
            result = cursor_obj.fetchall()
            result_col = cursor_obj.description
            print("{message : return db query result}")
            self.logger.info("steps in db manager")
            self.logger.info("message : conduct the sql => " + sql)
            conn.close()
            return result, result_col
        except PooledDBError as e:
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        
    
    def query_database_noresult(self, sql):
        try:
            self.pool = PooledDB(**self.db_config)
            conn = self.pool.connection()
            cursor_obj = conn.cursor()
            cursor_obj.execute(sql)
            conn.commit()
            conn.close()
        except PooledDBError as e:
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)