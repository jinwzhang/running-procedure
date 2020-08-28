import re
import datetime as dt
from dateutil.relativedelta import relativedelta

"""
Oracle 存储过程转义器
author: zhangjinwei
功能： 将常见的oracle if 条件判断式中的条件里的公式，转化成python语言，来在python里进行布尔判断
oracle 函数如to_date等，需要在本页自定义，可自由扩展
to_char(trunc(sysdate - 60, 'MM'), 'yyyymmdd') >= '20200111' 转换成 to_char(trunc(dt.datetime.now() + relativedelta(days=int(-60)), 'mm'), 'yyyymmdd') >= '20200111'
"""


def to_date(string: str, pattern: str):
    string, pattern = string.lower(), pattern.lower()
    pattern = pattern.replace('yyyy', '%Y')
    pattern = pattern.replace('yy', '%y')
    pattern = pattern.replace('mm', '%m')
    pattern = pattern.replace('dd', '%d')
    date = dt.datetime.strptime(string, pattern)
    return date


def to_char(date_time, pattern):
    pattern = pattern.lower()
    pattern = pattern.replace('yyyy', '%Y')
    pattern = pattern.replace('yy', '%y')
    pattern = pattern.replace('mm', '%m')
    pattern = pattern.replace('dd', '%d')
    date_string = dt.datetime.strftime(date_time, pattern)
    return date_string


def add_months(date_time, mon_number):
    assert int(mon_number) == mon_number, '与Oracle不同，本函数只支持整数加减，本次入参( %s )' % str(mon_number)
    return date_time + relativedelta(months=int(mon_number))


def trunc(date_time, pattern):
    pattern = pattern.upper()
    if pattern == 'D':
        return dt.date(date_time.year, date_time.month, date_time.day)
    elif pattern in ('M', 'MONTH', 'MM'):
        return dt.date(date_time.year, date_time.month, 1)


def sign_transform(string):
    """Oracle 符号转换函数， 目的是转换日期加减格式到python格式"""
    pattern1 = re.compile(r'(?:to_date|date)', re.IGNORECASE)  # 处理日期转换
    pattern2 = re.compile(r'([-+]) *(\d*)\b,')   # 处理加减日期法
    pattern3 = re.compile(r'(\bdate\b\s*?)(\'.*?\')', re.IGNORECASE)
    pattern4 = re.compile(r'([a-zA-Z0-9_ ]+)(=)([a-zA-Z0-9_ ]+)')  # 找单个的等号
    searched1, searched2, searched3, searched4 = pattern1.search(string), pattern2.search(string), pattern3.search(string), pattern4.search(string)
    if searched4:
        string = re.sub(pattern4, r'\1==\3', string)
    if searched1 and searched2:
        days = ''.join(searched2.groups())  # 提取加减日期数
        string = re.sub(pattern2, '+ relativedelta(days=int(%s)),' % days, string)  # 拼接成python能计算的日期加减公式
    if searched3:
        dt_str = searched3.group(2).replace('-', '').replace('\\', '')
        string = re.sub(pattern3, "to_date(%s, 'yyyymmdd')" % dt_str, string)
    string = string.replace('sysdate', 'dt.datetime.now()')
    return string


def if_analyser(string):
    """调用python的eval函数计算True false"""
    trans = sign_transform(string.strip().lower())
    # print('if_analyser>>', trans)
    boool = eval(trans)
    boool = 1 if boool else 0
    return boool


if __name__ == '__main__':
    s = "to_char(trunc(sysdate - 60, 'MM'), 'yyyymmdd') >= '20200111'"
    result = if_analyser(s)
    print(result)

