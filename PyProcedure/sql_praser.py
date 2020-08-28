import pandas as pd
import re
import json
import sys
from os import sep
if sys.platform == 'linux':
    import imp
    config = imp.load_source('config', '/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/pyProcedure/conf/config.py')
    sql_function = imp.load_source('sql_function', '/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/pyProcedure/sbin/sql_function.py')
    from config import ProcVariable
    import sql_function
else:
    from config import ProcVariable
    import sql_function
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)
pd.set_option('mode.chained_assignment', None)


def read_script(script_path, encoding='utf-8'):
    """用于从文本中读取单个存储过程，并调用清洁函数"""
    job_name = script_path.split(sep)[-1].split('.')[0]
    with open(script_path, 'r', encoding=encoding) as f:
        script_raw = f.read()
        return remove_quote(script_raw), job_name


def remove_quote(raw_text, all_quote=False):
    """清洁函数，去除特殊字符和多余字符"""
    if not all_quote:
        patern1 = re.compile(r'[\t\r\f\v]')
        raw_text = re.sub(patern1, ' ', raw_text)  # 去掉特殊占位符
        raw_text = re.sub(' +', ' ', raw_text)  # 去掉多余空格
        raw_text = re.sub('\s*\n+\s*', '\n', raw_text)  # 去掉多余换行
        patern2 = re.compile(r'/\*.+?\*/', re.DOTALL)  # 去除大块注释里的内容 /* XXXX */ 非贪婪
        raw_text = re.sub(patern2, '', raw_text)
        raw_text = raw_text.replace('；', ';').replace('，', ',').replace('（', '(').replace('）', ')')  # 替换常见半角
        patern3 = re.compile(r'(?:--|#).*?;+.*?\n')  # 去注释里包含了分号的这种注释 例如 'id --客户号;' eg: 12345
        raw_text = re.sub(patern3, '\n', raw_text)
        pattern_create_procedure = re.compile(r'(\s*CREATE .*?PROCEDURE.*?\(.*?\).*?AS)', flags=re.IGNORECASE | re.DOTALL)
        raw_text = re.sub(pattern_create_procedure, r'\1;', raw_text)   # 给create procedual 这句话后面添一个; 以免影响下一行
        pattern_begin = re.compile(r'(\n\s*BEGIN\s+)', flags=re.IGNORECASE)
        raw_text = re.sub(pattern_begin, r'\1;', raw_text)  # 给这句话后面添一个;进行断句以免影响下一行
        pattern_if_then = re.compile(r'(\n\s*(?:if|els?e?if)\s*?.*?\s*then)', flags=re.IGNORECASE)
        raw_text = re.sub(pattern_if_then, r'\1;', raw_text)  # 给这句话后面添一个;进行断句以免影响下一行
        # pattern_case_when_else_end = re.compile(r'(\bcase.*?\bwhen\b.*?\bthen\b.*?)(?!from.*)(\belse\b)(.*?\bend\b)', flags=re.IGNORECASE|re.DOTALL)
        # raw_text = re.sub(pattern_case_when_else_end, r'\1 EsCaPe_ThE_ElSe \3', raw_text)  # 先把case when里的else换走
        pattern_if_else = re.compile(r'(;\n\s*else\s*)', flags=re.IGNORECASE)
        raw_text = re.sub(pattern_if_else, r'\1;', raw_text)  # 把if 里的else 加个分号
        # raw_text = raw_text.replace(' EsCaPe_ThE_ElSe ', 'else')

    else:
        # 主要用在单个sql的时候，只能够清除内部的单行整行注释
        patern4 = re.compile(r'(?:--|#).*?\n')
        raw_text = re.sub(patern4, '\n', raw_text)

    return raw_text


class FormattedSql:
    """格式化sql类，将文本sql转化为一条条待执行sql, 并存放在表单中待执行"""
    def __init__(self, sql_path: str, v_date, db_type):
        self.raw_sql, self.job_name = read_script(sql_path)
        self.v_date = v_date
        self.db_type = db_type
        self.sql_list = self.raw_sql.split(';')
        self.sql_infer = [self.lines_infer(i) for i in self.sql_list]
        self.server = ''

    @property
    def defaut_variable(self):
        # 获取配置文件中对应的变量默认值
        variable_dict = ProcVariable(self.v_date, self.db_type).get_dict()
        variable_dict.pop('_i_tx_date')
        return variable_dict

    def sql_add_variables(self, one_sql, var_pool, exec=1):
        """
        功能是对单条sql变量字 替换成默认变量格式
        例如： sql内部所有的 v_tx_date 都会被替换成 to_date('20200814', 'YYYYMMDD') 忽略大小写
        传参逻辑： 通过python的字符串传参将对应的 sql数值字符串 传入
        """
        var_pool = json.loads(var_pool, encoding='uft-8')
        if exec:
            pattern = re.compile(r'\b(' + '|'.join(var_pool.keys()) + r')\b', flags=re.IGNORECASE)
            pattern_execute_immediate = re.compile(r'^\s*EXECUTE IMMEDIATE', flags=re.IGNORECASE)
            try:
                one_sql = pattern.sub(lambda x: var_pool[x.group().lower()], one_sql)  # 换入变量str
                if pattern_execute_immediate.search(one_sql):
                    one_sql = pattern_execute_immediate.sub('', one_sql)  # 去掉EXECUTE IMMEDIATE
                    one_sql = one_sql.strip()[1:-1]   # 截掉开头结尾的单引号 ' '
            except AttributeError:  # 如果没找到对应字，忽略错误
                pass
            exce_sql = one_sql.replace('`', '\'').replace('I_TX_DATE', '\'%s\'' % self.v_date).strip()
            return exce_sql
        else:
            return ' '

    def lines_infer(self, one_sql):
        """语句理解模块， 根据正则表达式匹配结果返回语句意思"""
        pattern_create_procedure = re.compile(r'^\s*CREATE .*?PROCEDURE.*?\(.*?\).*AS', flags=re.IGNORECASE | re.DOTALL)
        pattern_variable_setting = re.compile(r'(?:(\s*DEFAULT\s*)(.{1, 30})|(:=)\s*(.{1, 30})\n)', re.IGNORECASE)
        pattern_variable_declare = re.compile(r'^\s*V_.*', flags=re.IGNORECASE)
        pattern_select_into = re.compile(r'^\s*select\s+.*?(\s+into\s+)(.*)\s+from\s+.*', re.IGNORECASE | re.DOTALL)
        pattern_begin_end = re.compile(r'(?:^\s*BEGIN\s*$|^\s*END\s*$)', flags=re.IGNORECASE | re.DOTALL)
        pattern_com_p = re.compile(r'^\s*COM\..*', flags=re.IGNORECASE | re.DOTALL)
        pattern_exception_handel = re.compile(r'(?:.*exception.*when.*then.*|.*O_ERR_.* ?:=.*)', flags=re.IGNORECASE | re.DOTALL)
        pattern_commit = re.compile(r'^\s*commit$', flags=re.IGNORECASE)
        pattern_quote = re.compile(r'(?:--|#).*\n$')
        pattern_execute_immediate = re.compile(r'^\s*EXECUTE IMMEDIATE .*', flags=re.IGNORECASE | re.DOTALL)
        pattern_execute_insert = re.compile(r'^\s*INSERT (?:INTO|OVERWRITE).*', flags=re.IGNORECASE | re.DOTALL)
        pattern_execute_table_modify = re.compile(r'^\s*(?:CREATE|TRUNCATE|DROP) .*?(?:TABLE|VIEW).*?', flags=re.IGNORECASE | re.DOTALL)
        pattern_execute_function_modify = re.compile(r'^\s*(?:CREATE|TRUNCATE|DROP) .*?(FUNCTION).*?', flags=re.IGNORECASE | re.DOTALL)
        pattern_blank = re.compile(r'^\s*$', flags=re.IGNORECASE)
        pattern_if_then = re.compile(r'^\s*(?:if)\s*?(.*?)\s*then', flags=re.IGNORECASE)
        pattern_if_elsif_then = re.compile(r'^\s*(?:els?e?if)\s*?(.*?)\s*then', flags=re.IGNORECASE)
        pattern_if_else = re.compile(r'^\s*else\s*', flags=re.IGNORECASE)
        pattern_if_end = re.compile(r'^\s*end\s*if', flags=re.IGNORECASE)
        pattern_execute_hive_set = re.compile(r'^\s*set hive\..*', flags=re.IGNORECASE)
        phrase_dic = {'create_procedure': pattern_create_procedure, 'variable_setting': pattern_variable_setting,
                      'variable_declare': pattern_variable_declare, 'select_into': pattern_select_into, 'begin_end': pattern_begin_end,
                      'com_p': pattern_com_p, 'exception_handel': pattern_exception_handel, 'execute_commit': pattern_commit,
                      'quote': pattern_quote, 'execute_immediate': pattern_execute_immediate,'execute_insert': pattern_execute_insert,
                      'blank': pattern_blank, 'execute_table_modify': pattern_execute_table_modify, 'execute_hive_set': pattern_execute_hive_set,
                      'execute_function_modify': pattern_execute_function_modify,
                      'if_then': pattern_if_then, 'if_elsif_then': pattern_if_elsif_then, 'if_else': pattern_if_else, 'if_end': pattern_if_end}
        one_sql = remove_quote(one_sql, all_quote=True)  # 清除整行注释
        for type, pattern in phrase_dic.items():
            if re.search(pattern, one_sql):
                return type
        if len(one_sql) < 50:
            return 'UNKWNON'
        else:
            raise ValueError('类型未定义 %s' % one_sql)

    def variable_praser(self, sql, type):
        """变量提取函数，将变量提取成字典对， 然后以JSON输出"""
        if type == 'variable_setting':
            sql = remove_quote(sql, all_quote=True).strip()  # 移除注释
            sql = sql.replace('\'', '`')
            pattern1 = re.compile(r'^[A-Za-z0-9_]+')  # 找变量名
            pattern2 = re.compile(r'\s*(DEFAULT)\s*(.*)', re.IGNORECASE)  # 找变量赋值
            pattern3 = re.compile(r'(:=)\s*(.*)')
            searchobj1, searchobj2, searchobj3 = re.search(pattern1, sql), re.search(pattern2, sql), re.search(pattern3, sql)
            if searchobj1 and (searchobj2 or searchobj3):
                variable = searchobj1.group().lower()  # 强制小写
                value = str(searchobj2.group(2)).strip() if searchobj2 else str(searchobj3.group(2)).strip()
                pair = {variable: value}
                return json.dumps(pair, ensure_ascii=False)

    def if_clause_praser(self, sql, variable_pool):
        """if then elsif then else判断条件真假并返回"""
        pattern_if_then = re.compile(r'^\s*(if)\s*?(.*?)\s*then', flags=re.IGNORECASE)
        pattern_if_elsif_then = re.compile(r'^\s*(els?e?if)\s*?(.*?)\s*then', flags=re.IGNORECASE)
        sql = remove_quote(sql, all_quote=True).strip()  # 移除注释
        searched1, searched2 = pattern_if_then.search(sql), pattern_if_elsif_then.search(sql)
        if searched1:
            criteria = searched1.group(2)  # 获取if判断条件中的关键字比如 if v_tx_date > XXX then 则取 v_tx_date > XXX
        else:
            criteria = searched2.group(2)
        criteria = self.sql_add_variables(criteria, variable_pool)  # 将变量池中的变量换入，将v_tx_date换成to_date(指定日期,'yyyymmdd')
        return sql_function.if_analyser(criteria), criteria

    def exception_praser(self, sql):
        pass

    def select_into_praser(self, sql):
        """根据select into语句给变量赋值，需要将sql语句发送至数仓中计算结果"""
        sql = remove_quote(sql, all_quote=True)
        pattern_select_into = re.compile(r'^\s*(select\s+.*?)(\s+into\s+)(.*)\s+(from\s+.*)', re.IGNORECASE | re.DOTALL)
        searched = pattern_select_into.search(sql)
        select_sql = searched.group(1) + ' ' + searched.group(4)
        variables_name = searched.group(3).lower().split(',')
        return select_sql, variables_name

    @property
    def excute_plan(self):
        """生成原始的sql按语句进行的执行计划 返回dataframe格式"""
        plan_df = pd.DataFrame({'sql_all': self.sql_list, 'infer': self.sql_infer})
        plan_df['variable_infer'] = plan_df.apply(lambda row: self.variable_praser(row['sql_all'], row['infer']), axis=1)
        plan_df['variable_pool'] = ''
        for index, infer_type, pair, pool in zip(plan_df.index, plan_df['infer'], plan_df['variable_infer'], plan_df['variable_pool']):
            # 遍历并更新变量池
            previous_pool_json = plan_df.at[index-1, 'variable_pool'] if index > 0 else None
            previous_pool = json.loads(previous_pool_json, encoding='utf-8') if previous_pool_json else None

            if infer_type == 'variable_setting':
                pair = json.loads(pair, encoding='utf-8')
                if not previous_pool:
                    previous_pool = pair
                else:
                    pair_key = [i for i in pair.keys()][0]
                    pair_val = pair.get(pair_key)
                    pair_val = self.sql_add_variables(pair_val, previous_pool_json)  # 这步是为了将嵌套变量代入
                    previous_pool.update({pair_key: pair_val})
            new_pool = json.dumps(previous_pool, ensure_ascii=False)
            plan_df.at[index, 'variable_pool'] = new_pool

        plan_df['job_id'] = 0
        plan_df['tx_date'] = self.v_date
        plan_df['db_type'] = self.db_type
        plan_df['server'] = self.server
        plan_df['job_name'] = self.job_name
        plan_df['job_step'] = plan_df.index
        plan_df['step_status'] = ''
        plan_df['sql_send'] = ' '
        plan_df['if_bool_check'] = ''
        plan_df['group_number'] = 1
        plan_df['start_time'] = ''
        plan_df['end_time'] = ''
        plan_df['spend_time'] = ''
        plan_df['deal_row'] = 0
        plan_df['return_msg'] = ''
        plan_df['exception_handle'] = ''
        plan_df['exec_sign'] = 0
        # plan_df.to_excel(r'D:\DDL_BACKUP\text1.xlsx')
        return plan_df


if __name__=='__main__':

    pass

