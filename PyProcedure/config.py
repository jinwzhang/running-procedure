import datetime as dt

"""
Pyprocedure 配置文件，主要用来存储数仓登陆配置信息
"""
class ServerConfig:
    """
    服务器配置，根据不同服务器名返回不同的连接信息
    返回结果为 1 数据库类型， 2 登录用户， 3 登陆密码， 4 服务器 5 端口信息
    """
    def __init__(self, name):
        self.name = name.lower()

    def info(self):
        """返回结果需自行配置"""
        if self.name == 'edw':
            return 'Oracle', 'dwdba', 'dwdba', '10.1**.1**.72', '1521'
        elif self.name == 'hive':
            return 'hive', 'etluser', '', '10.***.***.78', '10000'
        elif self.name == 'mysql':
            return ''


class ProcVariable:
    """
    根据具体的数仓变量习惯，配置成具体的数仓常用变量, 暂时弃用
    """
    def __init__(self, v_date: str, DB_type='Oracle'):
        if DB_type == 'Oracle':
            self._I_TX_DATE = v_date
            self.v_job_name = ''
            self.v_job_desc = ''
            self.v_job_step = ''
            self.v_job_state = 'Done'
            self.v_job_state_desc = '处理成功'
            self.v_start_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.v_tx_date = "TO_DATE(%s, 'yyyymmdd')" % self._I_TX_DATE
            self.v_max_date = "to_date('2999-12-31', 'yyyy-mm-dd')"
            self.v_min_date = "to_date('1900-01-01', 'yyyy-mm-dd')"
            self.v_null_num = '0'
            self.v_null_str = "\'\'"
            self.v_null_date = "to_date('1900-01-02', 'yyyy-mm-dd')"
            self.v_begin_month = "TO_DATE(substr(%s, 1, 6) || '01', 'yyyymmdd')" % self._I_TX_DATE
            self.v_end_month = "last_day(%s)" % self.v_tx_date
            self.v_week_start = "trunc(%s, 'D')" % self.v_tx_date
            self.v_week_end = "trunc(%s, 'D') + 7" % self.v_tx_date
        else:
            pass

    def get_dict(self):
        dic = {}
        for key, val in self.__dict__.items():
            dic[key.lower()] = val
        return dic


if __name__ == '__main__':
    s = ProcVariable('20200202')
    print(s.__dict__)
