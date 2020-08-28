import sys
if sys.platform == 'linux':
    import imp
    DB_Buddy = imp.load_source('DB_Buddy', '/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/python_app/sbin/DB_Buddy.py')
    import DB_Buddy
else:
    from database import DB_Buddy
from pandas import DataFrame
'''
sql存储过程日志记录器，主要目的是实现中断记录，断点重跑，以及简版/详细版日志记录的功能。
'''


def clean_and_cut(string1):
    """将文本转化，为了可以放在Insert语句中"""
    if isinstance(string1, str):
        string = string1.replace('\"', '``').replace(',', '.').replace('\'','`').replace('(', '<').replace(')', '>')
        string = string[:1000]
        return string
    else:
        return string1


class EventKeeper:
    def __init__(self, db_type, server, job_name, tx_date):
        self.session = DB_Buddy.DatabaseBuddy('Oracle', 'dwdba', 'dwdba#123', '10.161.102.74', '1521/phdw')
        self.db_type = db_type
        self.server = server
        self.job_name = job_name
        self.tx_date = tx_date
        self.exsiting_log = None
        self.job_id = 0
        self.log_columns = []

    def get_job_record(self, table_name='COM.ETL_PYLOG_DETAIL'):
        """查找同个作业在本日的日志"""
        with self.session as cur:
            sql = 'SELECT * FROM %s WHERE JOB_NAME=\'%s\' AND TX_DATE=\'%s\' AND SERVER=\'%s\' AND DB_TYPE=\'%s\'' % (table_name, self.job_name, self.tx_date, self.server, self.db_type)
            cur.excute_one(sql)
            log_columns = cur.columns()
            self.log_columns = [str(val).lower() for val in log_columns]
            try:
                log_df = cur.to_dataframe()
            except AssertionError:
                log_df = None

            if isinstance(log_df, DataFrame):
                log_df.columns = [val.lower() for val in log_df.columns]
                job_id = int(log_df['job_id'].max())
                log_df = log_df[log_df['job_id'] == job_id]  # 取当日最近一条
                self.job_id = job_id
                self.exsiting_log = log_df
            return log_df

    def record(self, records_df, table_name='COM.ETL_PYLOG_DETAIL'):
        if table_name != 'COM.ETL_PYLOG_DETAIL':
            self.get_job_record(table_name)   # 如果本次不是默认的日志表，调用函数重新获取此日志表信息
            # records_df.to_excel(r'D:\DDL_BACKUP\text1.xlsx')
        job_id = self.job_id + 1 if self.job_id else 1  # 如运行过，则ID递增1
        records_df.loc[:, 'job_id'] = job_id
        record_columns = [str(val).lower() for val in records_df.columns]  # 取到拟记录日志的dataframe表头
        shared_columns = [val for val in self.log_columns if val in record_columns]  # 和日志数仓表头取并集 -- 为提高健壮性，如果日志表头略有不一致不中断程序
        not_fit_columns = [val for val in self.log_columns if val not in record_columns]
        if not_fit_columns:
            print('以下内容的日志，因表头不一致而未能记录-->', not_fit_columns)
        records = records_df.loc[:, shared_columns]
        columns = ', '.join(shared_columns)
        # 拼接INSERT语句操作
        sql = "INSERT ALL"
        for record in records.iterrows():
            # print(record, '>>>', type(record[1]))
            record = record[1].tolist()
            record = [clean_and_cut(i) for i in record]
            record = str(record)[1:-1]  # 去掉列表字符串化后的中括号
            sub_sql = "\nINTO %s (%s) VALUES (%s)" % (table_name, columns, record.replace('\"', '``').replace('(', '<').replace(')', '>'))
            sql += sub_sql
        sql += "\nSELECT * FROM dual"
        # print('log_sql>>>', sql)
        with self.session as cur:
            cur.excute_one(sql, commit=True)

    def record_detail(self, records_df):
        """记录每一行语句的详细日志"""
        self.get_job_record('COM.ETL_PYLOG_FULL_DETAIL')
        self.record(records_df, table_name='COM.ETL_PYLOG_FULL_DETAIL')


