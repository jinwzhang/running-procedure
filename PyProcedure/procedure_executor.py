import sys
import datetime as dt
import json
import traceback
import pdb
from pandas import DataFrame
if sys.platform == 'linux':
    import imp
    sql_praser = imp.load_source('sql_praser', '/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/pyProcedure/sbin/sql_praser.py')
    sql_event_manager = imp.load_source('sql_event_manager', '/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/pyProcedure/sbin/sql_event_manager.py')
    config = imp.load_source('config','/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/pyProcedure/conf/config.py')
    DB_Buddy = imp.load_source('DB_Buddy', '/etl_home/app/ETL_HOME/ETL_SERVER/BIN/script/python_app/sbin/DB_Buddy.py')
    import sql_praser
    import sql_event_manager
    import config
    import DB_Buddy
else:
    import sql_praser
    import sql_event_manager
    import config
    from database import DB_Buddy


"""
SQL模拟存储过程执行器，通过向对应数据库发送sql指令实现类似oracle中的存储过程模拟。并记录日志。
Author: Zhangjinwei
Date: 20200819
Version: v1.0
"""


def json_update(json_old, new_dic):
    """将字典的值更新到json串中"""
    if len(json_old) > 8 and isinstance(new_dic, dict):
        json_dic = json.loads(json_old, encoding='utf-8')
        json_dic.update(new_dic)
        json_new = json.dumps(json_dic, ensure_ascii=False)
        return json_new
    else:
        return json_old


def exception_handel(e):
    """错误捕捉"""
    print('程序报错：', e,dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    traceback.print_exc()  # 打印异常信息
    exc_type, exc_value, exc_traceback = sys.exc_info()
    return_msg = str(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))[:800]
    return return_msg


def seconds_to_hms(seconds: int) -> str:
    """时间格式转换"""
    hour, minute, second = seconds // 3600, seconds % 3600 // 60, seconds % 60
    strf_time = dt.datetime(2020, 1, 1, int(hour), int(minute), int(second)).strftime('%H:%M:%S')
    return strf_time


def re_run(log: DataFrame, option):
    warn_msg = """开始断点重跑>>>
    重跑逻辑：
    ！！！前提是已跑的后面的步骤不会影响之前步骤的数据：如表格重用，或删除了中间表的数据，则必须全部重跑，否则数据错误。
    1 如原脚本没有改动，那么从原断点处开始重跑
    2 原脚本有改变，那么从第一个修改处开始重跑 
    """
    if isinstance(log, DataFrame):
        status_code = log.loc['step_status'].to_list()


def excutor(db, exec_plan_cls, log_manager):
    """存储过程执行主函数"""
    dynamic_pool = {}
    exec_sign = 1
    log_df = log_manager.get_job_record()  # 获取本作业本日的最近一次执行记录
    return_code = 0
    with db:
        exec_plan_df = exec_plan_cls.excute_plan  # 生成分步执行计划
        exec_plan_df1 = exec_plan_df.copy(deep=True)
        exec_plan_df1.fillna(' ', inplace=True)
        for index, plan in exec_plan_df1.iterrows():
            start_time = dt.datetime.now()
            return_msg = ''
            sql_all = sql_praser.remove_quote(str(plan['sql_all']), all_quote=True)  # 移除注释
            sql = sql_all.strip() if sql_all else ' '
            infer, variable_pool = str(plan['infer']), str(plan['variable_pool'])
            variable_pool = json_update(variable_pool, dynamic_pool) if len(variable_pool) > 5 else ''  # 将动态执行得到的新变量更新进去
            if infer.startswith('execute_') and exec_sign:
                #  程序执行模块
                return_msg = '执行成功'
                affected_rows = 0
                try:
                    exec_sql = exec_plan_cls.sql_add_variables(sql, variable_pool) if len(variable_pool) > 5 else sql
                    affected_rows = db.excute_one(exec_sql)
                except Exception as e:
                    return_msg = exception_handel(e)
                exec_plan_df1.at[index, 'deal_row'] = affected_rows
                exec_plan_df1.at[index, 'exec_sign'] = 1
                sql = exec_sql
            elif infer == 'select_into' and exec_sign:
                #  select into变量动态赋值模块
                try:
                    select_sql, variable_list = exec_plan_cls.select_into_praser(sql)  # 根据语句拆成2部分， 1是要查询的语句， 2是要赋值的变量名
                    select_sql = exec_plan_cls.sql_add_variables(select_sql, variable_pool)  # 先替换入已知变量
                    db.excute_one(select_sql)  # 执行查询语句
                    var_df = db.to_dataframe(row_num=1)  # 获取查询结果
                    variable_value = var_df.values.tolist()[0]
                    assert len(variable_list) == len(variable_value), '变量数不一致，程序报错 variable_list:%s variable_value:%s' % (str(variable_list), str(variable_value))
                    variable_pool_dic = json.loads(variable_pool, encoding='utf-8')
                    variable_dic = {}
                    for key1, val1 in zip(variable_list, variable_value):
                        variable_dic.update({key1: val1})
                    variable_pool_dic.update(variable_dic)   # 1更新变量值
                    exec_plan_df1.at[index, 'variable_infer'] = json.dumps(variable_dic, ensure_ascii=False)  # 2
                    sql = select_sql
                    exec_plan_df1.at[index, 'exec_sign'] = 1
                except Exception as e:
                    return_msg = exception_handel(e)
            elif infer.startswith('if_'):
                # if else功能主模块
                criteria = ' '
                if infer == 'if_then':
                    bool_value, criteria = exec_plan_cls.if_clause_praser(sql, variable_pool)
                    exec_sign = bool_value
                elif infer == 'if_elsif_then' and not exec_sign:
                    bool_value, criteria = exec_plan_cls.if_clause_praser(sql, variable_pool)
                    exec_sign = bool_value
                elif infer == 'if_else' and not exec_sign:
                    exec_sign = 1
                elif infer == 'if_end':
                    exec_sign = 1
                exec_plan_df1.at[index, 'exec_sign'] = exec_sign
                exec_plan_df1.at[index, 'if_bool_check'] = criteria

            end_time = dt.datetime.now()
            exec_time = (end_time - start_time).seconds
            exec_time = seconds_to_hms(exec_time)
            exec_plan_df1.at[index, 'start_time'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
            exec_plan_df1.at[index, 'return_msg'] = return_msg
            exec_plan_df1.at[index, 'end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
            exec_plan_df1.at[index, 'spend_time'] = exec_time
            if sql:
                sql = sql.replace('\'', '`').replace('\"', '``').replace('\n', 'char(10)')
                exec_plan_df1.at[index, 'sql_short'] = sql[:100]
                exec_plan_df1.at[index, 'sql_send'] = sql[:800]
            if infer.startswith('execute_') or infer.startswith('if_') or infer in ('variable_setting', 'select_into'):
                # 记录简版日志到数仓
                log_manager.record(exec_plan_df1.iloc[[index]])
            if return_msg not in ('', '执行成功') and plan['exception_handle'] != 'ignore':
                # 错误中断检查
                print('运行停止， 错误信息 ', return_msg, '>>>%s' % plan['sql_send'], sql)
                exec_plan_df1.at[index, 'step_status'] = 'ERROR'
                exec_plan_df1['variable_pool'].apply(lambda x: json_update(x, dynamic_pool))   # 出错后保留变量状态
                # exec_plan_df1.drop(columns=['sql_all'], inplace=True)
                log_manager.record_detail(exec_plan_df1)
                return_code = 1
                break

        log_manager.record_detail(exec_plan_df1)  # 记录所有日志
        # exec_plan_df1.to_excel(r'D:\DDL_BACKUP\text.xlsx')
        return return_code


def run(tx_date, db_name, script_addr):
    """主调用函数"""
    t0 = dt.datetime.now()
    print('<< PyProcedure V1.0 >> by Zlaw\n')
    print('%s  程序开始 ' % t0.strftime('%Y-%m-%d %H:%M:%S'), '\n批次日期: ', tx_date, '数仓名: ', db_name, '\n存储过程路径: ', script_addr)
    DB_type, db_username, db_pwd, db_IP, db_port = config.ServerConfig(db_name).info()  # 获取配置信息
    db = DB_Buddy.DatabaseBuddy(DB_type, db_username, db_pwd, db_IP, db_port, option=16)  # 连接数据库
    exec_plan = sql_praser.FormattedSql(script_addr, tx_date, DB_type)  # 制作执行计划
    exec_plan.server = db_IP
    event_manager = sql_event_manager.EventKeeper(DB_type, db_IP, exec_plan.job_name, tx_date)  # 初始化日志记录器
    return_code = excutor(db, exec_plan, event_manager)  # 开始执行
    t1 = dt.datetime.now()
    totol_time = seconds_to_hms((t1 - t0).seconds)
    print('%s  程序结束' % t1.strftime('%Y-%m-%d %H:%M:%S'), '返回代码: ', return_code, '总运行时间: ', totol_time)
    assert return_code == 0, '程序运行错误'


if __name__ == '__main__':
    if sys.platform == 'linux':
        try:
            v_tx_date = sys.argv[1]
            db_name = sys.argv[2]
            sql_script_path = sys.argv[3]
        except:
            print('参数填写错误')
            raise ValueError('入参一：v_tx_date 如 20200819, 入参二：数据库名 如 edw hive等, 入参三：拟运行sql文本路径')

        try:
            other_option = sys.argv[4]
            print('可选参数已设置为 %', other_option)
        except:
            other_option = None
        run(v_tx_date, db_name, sql_script_path)

    else:
        script_path = r'D:\DDL_BACKUP\Oracle_DDL\ARPT\PROCEDURE\P_EXT_LOANINVOICE_EVENT_20200323_0956.sql'
        run('20200827', 'edw', script_path)


