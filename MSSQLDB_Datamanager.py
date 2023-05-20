from MSSQLDB_manager import Manager

class DataManager:
    def __init__(self, logger, config, time_start ,time_end):
        self.logger = logger
        self.config = config
        self.start = time_start
        self.end = time_end

    def read_data_from_db(self, TableName):
        manager = Manager(self.logger, self.config ,self.start ,self.end)
        if TableName == 'Behavior':
            self.Behavior = manager.get_db_table('Behavior',creat_dt = True)
            self.logger.info('Finish loading data from DB')
            return self.Behavior
        elif TableName == 'MemberData': 
            self.MemberData = manager.get_db_table('MemberData')
            self.logger.info('Finish loading data from DB')
            return self.MemberData
        elif TableName == 'OrderData':
            self.OrderData = manager.get_db_table('OrderData',creat_dt = True)
            self.logger.info('Finish loading data from DB')
            return self.OrderData
        elif TableName == 'OrderSlave':
            self.OrderSlave = manager.get_db_table('OrderSlave',creat_dt = True)
            self.logger.info('Finish loading data from DB')
            return self.OrderSlave
        elif TableName == 'SalePageData':
            self.SalePageData = manager.get_db_table('SalePageData')
            self.logger.info('Finish loading data from DB')
            return self.SalePageData
        elif TableName == 'SegmentData':
            self.SegmentData = manager.get_db_table('SegmentData')
            self.logger.info('Finish loading data from DB')
            return self.SegmentData
        
    def read_ALL_data_from_db(self):
        manager = Manager(self.logger, self.config ,self.start ,self.end)
        self.Behavior = manager.get_db_table('Behavior',creat_dt = True)
        self.MemberData = manager.get_db_table('MemberData')
        self.OrderData = manager.get_db_table('OrderData',creat_dt = True)
        self.OrderSlave = manager.get_db_table('OrderSlave',creat_dt = True)
        self.SalePageData = manager.get_db_table('SalePageData')
        self.SegmentData = manager.get_db_table('SegmentData')
        self.logger.info('Finish loading data from DB')
        return self.Behavior,self.MemberData,self.OrderData,self.OrderSlave,self.SalePageData,self.SegmentData