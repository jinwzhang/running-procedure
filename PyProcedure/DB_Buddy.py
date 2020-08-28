import cx_Oracle
import pyodbc
import pymysql
import pandas as pd
import re
import sys
if sys.platform == 'linux':
    from pyhive import hive

'''
<<<DB_Buddy v1.2>>>
功能: 
v1.0 20200402 作为python连接数仓的集成基础类使用， 在一个类提供ORACLE, MYSQL, SQLSERVER的直连操作。
v1.1 20200813 追加记录 SQL%ROWCOUNT 功能
v1.2 20200824 支持连接hive
并能将产生的查询转换成pandas.Dataframe，提供自带的context manager功能，方便with操作
作者: zhangjinwei
path: /etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/python_app

'''


def find_row_affect(logs: list):
    """正则函数， 目的是从大量的日志中找到 写入的行数信息"""
    pattern = re.compile(r'(HDFS Write: )(\d*)')
    search_record = [pattern.search(str(log)) for log in logs]
    for searched in search_record:
        if searched:
            return searched.group(2)
    return 0


class DatabaseBuddy(object):
    # 实例化入参： 1数据库类型 2用户名 3密码 4数据库IP 5端口号
    def __init__(self, DB_type, db_username, db_pwd, db_IP, db_port, **kwargs):
        self.DB_type = str(DB_type).upper()
        self._support_db = ['ORACLE', 'MYSQL', 'MSSQL', 'SQLSERVER', 'HIVE']
        assert self.DB_type in self._support_db, '数据库类型不支持, 目前支持%s' % str(self._support_db)
        self.db_username = db_username
        self._db_pwd = db_pwd
        self.db_IP = db_IP
        self.db_port = db_port
        self.other = kwargs
        self.parallel = kwargs.get('option', False)

    def __enter__(self):
        if self.DB_type == 'ORACLE':
            connection = cx_Oracle.connect(self.db_username, self._db_pwd, self.db_IP + ':' + self.db_port,
                                           encoding="UTF-8")
        elif self.DB_type == 'MYSQL':
            connection = pymysql.connect(user=self.db_username, password=self._db_pwd, host=self.db_IP,
                                         port=int(self.db_port), charset="utf8")
        elif self.DB_type == 'MSSQL':
            connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};server=%s;UID=%s;PWD=%s' % (
            self.db_IP + ',' + self.db_port, self.db_username, self._db_pwd))
            connection.add_output_converter(-150, self.handle_sql_variant_as_string)  # mssql部分返回值数据类型需转换后才能fetch
        elif self.DB_type == 'HIVE':
            connection = hive.Connection(host=self.db_IP, port=self.db_port, username=self.db_username)

        self.connections = connection
        cursor = connection.cursor()
        self.curr = cursor
        if self.DB_type == 'ORACLE' and int(self.parallel) in range(1, 20):
            self.excute_one('alter session enable parallel DML')
            self.excute_one('alter session force parallel query parallel %d' % self.parallel)
            self.excute_one('alter session force parallel dml parallel %d' % self.parallel)
            print('set oracle session with paralle %d' % self.parallel)
        return self

    def __repr__(self):
        return "<%s  %s  %s>" % (self.db_IP, self.db_port, self.db_username)

    def __exit__(self, exc_ty, exc_val, tb):
        self.connections.close()

    def handle_sql_variant_as_string(self, value):
        return value.decode('utf-16le')

    def fetch(self, limit=50000, printf=True) -> tuple:
        data = self.curr.fetchmany(limit)
        if printf:
            print(data)
        return data

    def excute_one(self, sql, commit=False, printf=False):
        """执行单条sql"""
        row_affect = self.curr.execute(sql)   # 适用于pymysql直接返回影响行数
        if printf:
            self.fetch()
        if commit:
            self.curr.execute('commit')
        if self.DB_type == 'ORACLE':
            row_affect = self.curr.rowcount
        elif self.DB_type == 'HIVE':
            log = self.fetch_logs()
            row_affect = find_row_affect(log)

        return row_affect

    def excute_one_asyn(self):
        """异步执行"""
        pass

    def columns(self) -> list:
        """返回sql结果的表头"""
        return [i[0] for i in self.curr.description]

    def run_procedual(self, proc_name: str, *arg):
        """运行存储过程，存储过程名，[存储过程参数]"""
        result_code = self.curr.var(int)
        self.curr.callproc(proc_name, arg)
        return result_code.getvalue()

    def to_dataframe(self, *, row_num=0, header=True):
        """将SQL执行的结果放入到DataFrame当中并返回"""
        assert row_num >= 0 and isinstance(row_num, int), \
            'row_num err. (0 = get 50000 data, int > 1 is get row_num of data), input=<%s>' % str(row_num)

        def instance_check(iterable1):  # 这一步是为了适应SQL SERVER的返回，是一种自定义数据类型，不是元组
            if isinstance(iterable1[-1], tuple):
                return iterable1
            else:
                iterable1 = [tuple(i) for i in iterable1]
                return iterable1

        if row_num == 0:
            data = self.fetch(printf=False)
            assert data, 'No data returned!'
            data = instance_check(list(data))
            df = pd.DataFrame(list(data))
        else:
            data = self.fetch(row_num, printf=False)
            assert data, 'No data returned!'
            data = instance_check(list(data))
            df = pd.DataFrame(data)
        if header:
            df.columns = self.columns()
        return df

    def fetch_logs(self):
        """新增读取hive日志"""
        if self.DB_type == 'HIVE':
            return self.curr.fetch_logs()

if __name__ == '__main__':
    pass

