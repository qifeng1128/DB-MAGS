import logging
import multiprocessing
import os
import subprocess
import random
import sys
import threading
from datetime import datetime
from time import sleep

import rand
from pprint import pformat
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
import getopt
from multiprocessing import Process
from threading import Thread
from tpcc_operation_set import table_lock_sqls, meta_data_sqls, record_lock_sqls, missing_index, too_much_index, implicit_conversion, \
    query_with_too_much_join, order_by, group_by, query_whole_table, lock_slow_query, mysql_dump, mysql_dump_cause

from tpcc import doOne, executeTransaction

from Connection.Connection import Database

#  Order constants
MIN_CARRIER_ID = 0  # 修改tpcc工作负载
MAX_CARRIER_ID = 10  # 修改tpcc工作负载

'''
----------------------------------------------------------------------
----------------------------------------------------------------------
'''


# 可选择是否指定伴随异常的子类型，不选择默认随机挑选执行
def get_sql_command(one_fault, ordered_fault_type):
    fault_type = ""
    effect_inject_sql = ""
    cause_inject_sql = ""
    # 因果关系（不可进行指定）
    if "--->" in one_fault:
        if one_fault == "lock--->slow":
            fault_type = "table_lock---slow_sql"
            # 进行慢查询和锁冲突设计
            lock_sqls, slow_query = lock_slow_query()
            effect_inject_sqls = slow_query
            cause_inject_sqls = lock_sqls
            index = rand.number(0, len(cause_inject_sqls) - 1)
            cause_inject_sql = cause_inject_sqls[index]
            effect_inject_sql = effect_inject_sqls[index]
        elif one_fault == "dump--->lock":
            fault_type = "dump---record_lock"
            # 进行数据表备份sql设计
            dump_sqls = mysql_dump_cause()
            cause_inject_sqls = dump_sqls
            index = rand.number(0, len(cause_inject_sqls) - 1)
            cause_inject_sql = cause_inject_sqls[index]
        elif one_fault == "flow--->lock":
            fault_type = "flow_workload---record_lock"
            cause_inject_sql = "flow_workload"  # 用于后续进行参数的修改
        elif one_fault == "flow--->resource":
            fault_type = "flow_workload---resource"
            cause_inject_sql = "flow_workload"  # 用于后续进行参数的修改
        else:
            fault_type = "resource_cpu---record_lock"  # todo 是否只有cpu的资源瓶颈能引起因果异常
            cause_inject_sql = f"/root/ChaosBlade/chaosblade-0.3.0/blade create cpu fullload"

    # 伴随关系（可进行指定）
    # '''
    # 伴随异常的大类及其对应的可选择子类      #### 假定伴随异常无法选择因果关系的异常类型
    # #### table_lock ---> slow_sql 由于需要构造辅助sql才能触发因果关系，因此不用进行排除
    # lock    table_lock, meta_data_lock, record_lock
    # flow    sql_flow      #### flow_workload ---> record_lock 无法选择整体工作负载的流量突增
    # slow    missing_index, too_much_index, implicit_conversion, query_with_too_much_join, order_by, group_by, query_whole_table
    # resource    io, disk, mem, net     #### resource_cpu ---> slow_sql 无法选择cpu的资源瓶颈
    # #### dump ---> record_lock 无法选择部分的数据表备份
    # '''
    else:
        if one_fault == "lock":
            fault_list = ["table_lock", "meta_data_lock", "record_lock"]
            if len(ordered_fault_type) != 0 and ordered_fault_type in fault_list:  # 进行指定
                fault_index = fault_list.index(ordered_fault_type)
                fault_type = ordered_fault_type
            else:  # 随机选择
                fault_index = rand.number(0, len(fault_list) - 1)
                fault_type = fault_list[fault_index]
            if fault_index == 0:
                sqls = table_lock_sqls()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 1:
                sqls = meta_data_sqls()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            else:
                lock_ratio = 0.8
                sqls = record_lock_sqls(lock_ratio)
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
        elif one_fault == "flow":
            fault_type = "flow_sql"
            effect_inject_sql = "flow_sql"
        elif one_fault == "slow":
            fault_list = ["missing_index", "too_much_index", "implicit_conversion", "query_with_too_much_join",
                          "order_by", "group_by", "query_whole_table"]
            if len(ordered_fault_type) != 0 and ordered_fault_type in fault_list:  # 进行指定
                fault_index = fault_list.index(ordered_fault_type)
                fault_type = ordered_fault_type
            else:  # 随机选择
                fault_index = rand.number(0, len(fault_list) - 1)
                fault_type = fault_list[fault_index]
            if fault_index == 0:
                sqls = missing_index()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 1:
                sqls = too_much_index()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 2:
                sqls = implicit_conversion()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 3:
                sqls = query_with_too_much_join()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 4:
                sqls = order_by()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 5:
                sqls = group_by()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            else:
                sqls = query_whole_table()
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
        elif one_fault == "resource":
            fault_list = ["io", "disk", "mem", "net", "cpu"]
            if len(ordered_fault_type) != 0 and ordered_fault_type in fault_list:  # 进行指定
                fault_index = fault_list.index(ordered_fault_type)
                fault_type = ordered_fault_type
            else:  # 随机选择
                fault_index = rand.number(0, len(fault_list) - 1)
                fault_type = fault_list[fault_index]
            if fault_index == 0:
                sqls = [f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk burn --read --path /home",
                        f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk burn --write --path /home",
                        f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk burn --read --write"]
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            elif fault_index == 1:
                effect_inject_sql = f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk fill --path /home --size 70000"
            elif fault_index == 2:
                effect_inject_sql = f"/root/ChaosBlade/chaosblade-0.3.0/blade c mem load --mem-percent 95"
            elif fault_index == 3:
                sqls = [
                    f"/root/ChaosBlade/chaosblade-0.3.0/blade create network delay --time 5000 --offset 1000 --interface eth0 --local-port 3306",
                    f"/root/ChaosBlade/chaosblade-0.3.0/blade create network loss --percent 90 --interface eth0 --local-port 3306"]
                sql_index = rand.number(0, len(sqls) - 1)
                effect_inject_sql = sqls[sql_index]
            else:
                effect_inject_sql = f"/root/ChaosBlade/chaosblade-0.3.0/blade create cpu fullload"
        else:
            sqls = mysql_dump()
            sql_index = rand.number(0, len(sqls) - 1)
            effect_inject_sql = sqls[sql_index]
            fault_type = "dump"

    return fault_type, cause_inject_sql, effect_inject_sql


def execute(duration, fault_inject_time, sleep_time, fault_one_type, fault_multi_type, fault_child_type,
            total_cause_fault_number, total_effect_fault_number, cause_inject_time, root_cause_path):
    '''
    :param duration:设置一直执行tpcc的时间长度（以秒为单位）
    :param thread_number:设置并发执行事务的线程数
    '''
    logging.info("Executing benchmark for %d seconds" % duration)

    # 创建线程池
    max_pool = 500
    max_thread = 300
    pool = ThreadPoolExecutor(max_workers=max_pool)
    pool_results = []

    start = datetime.now()

    # 异常类型和对应的注入sql/command
    total_fault_type = []  # 用于记录故障注入的类型
    total_fault_sql_command = []  # 用于记录故障注入的根因

    # 注入故障的总次数（通过注入sql的数量来调节症状指标的变化）
    total_fault_count1 = 0
    total_fault_count2 = 0

    # 注入的因果异常集合
    effect_inject_sql = []
    cause_inject_sql = []

    # 创建文件夹
    if not os.path.exists(root_cause_path):
        os.makedirs(root_cause_path)

    # 优先执行具有因果关系的故障，最后执行伴随关系的故障
    for one_fault in fault_multi_type:
        # 获取注入的子异常类型、sql或者命令行
        fault_type, one_cause_sql, one_effect_sql = get_sql_command(one_fault, [])  # 因果关系的异常子类无需指定
        total_fault_type.append(fault_type)
        if one_effect_sql == "":
            total_fault_sql_command.append([one_cause_sql])
            cause_inject_sql.append(one_cause_sql)
        else:
            total_fault_sql_command.append([one_cause_sql, one_effect_sql])
            effect_inject_sql.append(one_effect_sql)
            cause_inject_sql.append(one_cause_sql)

    for i in range(len(fault_one_type)):
        # 获取注入的子异常类型、sql或者命令行
        fault_type, one_cause_sql, one_effect_sql = get_sql_command(fault_one_type[i], fault_child_type[i])
        total_fault_type.append(fault_type)
        total_fault_sql_command.append([one_effect_sql])
        effect_inject_sql.append(one_effect_sql)

    # 将故障类型和对应的根因写入文件中
    for i in range(len(total_fault_type)):
        fault_path = root_cause_path + "/" + total_fault_type[i] + ".txt"
        # 打开文件并获取文件对象
        file = open(fault_path, "w")
        for j in range(len(total_fault_sql_command[i])):
            file.write(total_fault_sql_command[i][j] + "\n")
        file.close()

    # 故障注入（根据因果注入时刻不同分别批量注入）
    while (datetime.now() - start).seconds <= duration:
        print("time", (datetime.now() - start).seconds)

        # 触发为 因 的故障注入（设定为触发为 果 的故障注入时刻的0.5，即一般情况下为第30秒）
        if cause_inject_time < (
                datetime.now() - start).seconds < fault_inject_time and total_fault_count2 < total_cause_fault_number:  # count
            for one_inject_sql in cause_inject_sql:
                print("cause_fault:", one_inject_sql)
                if one_inject_sql == "flow_workload":
                    # 通过参数提高工作负载的流量
                    sleep_time = 0.001
                    max_thread = 500
                else:
                    # 注入故障的客户端
                    if "mysqldump" in one_inject_sql or "ChaosBlade" in one_inject_sql:
                        future = pool.submit(execute_command, one_inject_sql)  # 服务器执行故障命令行
                    else:
                        future = pool.submit(Fault_Session, one_inject_sql)  # 客户端执行故障sql
                    # 获取线程返回的结果
                    pool_results.append(future)
            # 每次注入故障sql后数量增加1
            total_fault_count2 = total_fault_count2 + 1
            print("cause_fault_count:", total_fault_count2)

            # 剩下的线程仍然执行tpcc负载
            if len(pool_results) < max_thread * 0.8:
                pool_results.append(pool.submit(Session))
                sleep(random.random() * sleep_time)  # t代替的位置是原来的thread_num
            else:
                for future in as_completed(pool_results):
                    pool_results.remove(future)
                    pool_results.append(pool.submit(Session))
                    sleep(random.random() * sleep_time)
                    break

        # 在注入 因 产生变化之后，进行 果 故障sql的注入
        if (
                datetime.now() - start).seconds > fault_inject_time and total_fault_count1 < total_effect_fault_number:  # count
            for one_inject_sql in effect_inject_sql:
                print("effect_fault:", one_inject_sql)
                if one_inject_sql == "flow_sql":
                    # 通过参数每个故障注入的次数提高sql的流量
                    total_effect_fault_number = 17
                else:
                    # 注入故障的客户端
                    if "mysqldump" in one_inject_sql or "ChaosBlade" in one_inject_sql:
                        future = pool.submit(execute_command, one_inject_sql)  # 服务器执行故障命令行
                    else:
                        future = pool.submit(Fault_Session, one_inject_sql)  # 客户端执行故障sql
                    # 获取线程返回的结果
                    pool_results.append(future)
            # 每次注入故障sql后数量增加1
            total_fault_count1 = total_fault_count1 + 1
            print("effect_fault_count:", total_fault_count1)

            # 剩下的线程仍然执行tpcc负载
            if len(pool_results) < max_thread * 0.8:
                pool_results.append(pool.submit(Session))
                sleep(random.random() * sleep_time)  # t代替的位置是原来的thread_num
            else:
                for future in as_completed(pool_results):
                    pool_results.remove(future)
                    pool_results.append(pool.submit(Session))
                    sleep(random.random() * sleep_time)
                    break

        else:
            if len(pool_results) < max_thread * 0.8:
                pool_results.append(pool.submit(Session))
                sleep(random.random() * sleep_time)  # t代替的位置是原来的thread_num
            else:
                for future in as_completed(pool_results):
                    pool_results.remove(future)
                    pool_results.append(pool.submit(Session))
                    sleep(random.random() * sleep_time)
                    break


def execute_command(command):
    subprocess.call(command, shell=True)


def Fault_Session(inject_sql):
    db = Database()
    conn, cur = db.connection2()
    cur.execute(inject_sql)
    sleep(5)
    conn.commit()
    conn.close()


def Session():
    db = Database()
    # 随机选择一种事务类型并执行
    conn, cur = db.connection2()
    # 随机选择一种事务类型并执行
    txn, params = doOne()
    try:
        val = executeTransaction(txn, params, conn)
        conn.commit()
    except Exception as e:
        print("Error in committing transaction: %s" % e)
        print(txn, params)
        print("rollback")
        conn.rollback()
    conn.close()


## DEF

def main(argv):
    duration = 0
    fault_inject_time = 0
    sleep_time = 0
    fault_one_type = []  # 伴随关系异常
    fault_multi_type = []  # 因果关系异常
    fault_child_type = []  # 伴随关系异常子类（若自定义的子类异常与因果异常相冲突，将自行随机选择可行的）
    total_cause_fault_number = 0
    total_effect_fault_number = 0
    root_cause_path = ""
    cause_inject_time = 30
    try:
        # 处理传入的参数内容
        opts, args = getopt.getopt(argv, "hd:d:t:i:k:s:c:e:p:x:")
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-d':
            duration = int(arg)
        if opt == '-t':
            fault_inject_time = int(arg)
        if opt == '-i':
            # 获取伴随关系和因果关系的多异常
            total_fault_type = arg
            fault_type = total_fault_type.split("+")
            for one_fault in fault_type:
                if "--->" in one_fault:
                    fault_multi_type.append(one_fault)
                else:
                    fault_one_type.append(one_fault)
        if opt == '-k':
            total_fault_child_type = arg
            fault_child_type = total_fault_child_type.split("+")
        if opt == '-s':
            sleep_time = float(arg)
        if opt == '-c':
            total_cause_fault_number = int(arg)
        if opt == '-e':
            total_effect_fault_number = int(arg)
        if opt == '-p':
            root_cause_path = str(arg)
        if opt == '-x':
            cause_inject_time = int(arg)


    execute(duration, fault_inject_time, sleep_time, fault_one_type, fault_multi_type, fault_child_type,
            total_cause_fault_number, total_effect_fault_number, cause_inject_time, root_cause_path)


if __name__ == "__main__":
    # -d 正常tpcc负载持续运行时间(s)
    # -t 注入果关系故障的时刻(一般设置为第60s)
    # -i 故障注入的类型（大类） 【伴随关系中优先写flow】
    # -k 故障注入的类型（对应的子类，仅限于伴随故障的类型选择）
    # -s 每个任务提交之后的休眠时间(s)
    # -c 故障注入任务的数量（为因关系的故障）
    # -e 故障注入任务的数量（为果关系的故障 / 单一类型故障）
    # -p 存储故障根因的路径
    # -x 注入因关系故障的时刻（一般设置为第30s）
    # args = ['-d', '150', '-t', '60', '-i', 'dump', '-k', 'dump', '-s', '0.044', '-c', '7', '-e', '7',
    #         '-p', '/root/mysqlrc/knowledge_graph/monitor/prometheus/10-11-22-38-47', '-x', '55']
    # main(args)
    main(sys.argv[1:])
