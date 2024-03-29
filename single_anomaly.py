import logging
import multiprocessing
import subprocess
import os
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
from tpcc import doOne, executeTransaction

from tpcc_operation_set import table_lock_sqls, meta_data_sqls, record_lock_sqls, missing_index, too_much_index, implicit_conversion, \
    query_with_too_much_join, order_by, group_by, query_whole_table, lock_slow_query, mysql_dump, cpu_bottle, io_bottle, disk_bottle, \
    mem_bottle, net_bottle

from Connection.Connection import Database

#  Order constants
MIN_CARRIER_ID = 0  # 修改tpcc工作负载
MAX_CARRIER_ID = 10  # 修改tpcc工作负载

'''
----------------------------------------------------------------------
----------------------------------------------------------------------
'''

def execute(duration, fault_inject_time, sleep_time, fault_type, total_cause_fault_number, total_effect_fault_number, cause_inject_time, root_cause_path):
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
    total_fault_count = 0       # 注入故障的总次数（通过注入sql的数量来调节症状指标的变化）
    total_fault_count1 = 0
    total_fault_count2 = 0

    # 注入方式的区分（数据库sql注入 & 服务器上命令行注入 & 调节参数方式注入）
    command_inject_flag = 0
    traffic_inject_flag = 0

    # 因果关系的多异常（判断是否注入为 果 的异常） / 单一异常是否注入
    effect_inject_flag = 0         # 故障注入需要执行设定的sql
    effect_inject_sql = ""
    effect_path = root_cause_path + "/effect_inject_sql.txt"

    # 因果关系的多异常（判断是否注入为 因 的异常）
    workload_inject_flag = 0    # 故障注入需要更改工作负载
    min_change_value = 0
    max_change_value = 10

    cause_inject_flag = 0      # 故障注入需要执行设定的sql
    cause_inject_sql = ""
    cause_path = root_cause_path + "/cause_inject_sql.txt"

    # 创建文件夹
    if not os.path.exists(root_cause_path):
        os.makedirs(root_cause_path)

    # 从故障注入层获取故障注入sql,每次仅注入一条相同故障sql，即根因sql
    # 获取对应故障类型的sql集合，从中随机选择一条
    cause_inject_sqls = []
    effect_inject_sqls = []

    if fault_type == 1:      # 表锁
        effect_inject_sqls = table_lock_sqls()
        effect_inject_flag = 1
    elif fault_type == 2:    # 元数据锁
        effect_inject_sqls = meta_data_sqls()
        effect_inject_flag = 1
    elif fault_type == 3:    # 行锁 & 慢增删改sql
        slow_lock_ratio = 0.01
        lock_ratio = 0.8
        effect_inject_sqls = record_lock_sqls(slow_lock_ratio)
        effect_inject_flag = 1
    elif fault_type == 4:    # 索引缺失
        effect_inject_sqls = missing_index()
        effect_inject_flag = 1
    elif fault_type == 5:    # 索引过多 & 行锁冲突
        effect_inject_sqls = too_much_index()
        effect_inject_flag = 1
    elif fault_type == 6:    # 隐式转换
        effect_inject_sqls = implicit_conversion()
        effect_inject_flag = 1
    elif fault_type == 7:    # join多表
        effect_inject_sqls = query_with_too_much_join()
        effect_inject_flag = 1
    elif fault_type == 8:    # order by
        effect_inject_sqls = order_by()
        effect_inject_flag = 1
    elif fault_type == 9:    # group by
        effect_inject_sqls = group_by()
        effect_inject_flag = 1
    elif fault_type == 10:   # 大表扫描
        effect_inject_sqls = query_whole_table()
        effect_inject_flag = 1
    elif fault_type == 11:   # 数据表备份
        # 执行数据库表备份（由于mysqldump命令需要输入密码，这里将密码显示在命令行中）
        effect_inject_sqls = mysql_dump()
        effect_inject_flag = 1
        command_inject_flag = 1
    elif fault_type == 12:   # cpu资源瓶颈
        effect_inject_sqls = cpu_bottle()
        effect_inject_flag = 1
        command_inject_flag = 1
    elif fault_type == 13:   # io资源瓶颈
        effect_inject_sqls = io_bottle()
        effect_inject_flag = 1
        command_inject_flag = 1
    elif fault_type == 14:   # disk资源瓶颈
        effect_inject_sqls = disk_bottle()
        effect_inject_flag = 1
        command_inject_flag = 1
    elif fault_type == 15:   # mem资源瓶颈
        effect_inject_sqls = mem_bottle()
        effect_inject_flag = 1
        command_inject_flag = 1
    elif fault_type == 16:   # net资源瓶颈
        effect_inject_sqls = net_bottle()
        effect_inject_flag = 1
        command_inject_flag = 1
    elif fault_type == 17:   # 工作负载的变化
        # 进行tpcc负载的变化（字段范围值更改）
        max_change_value = 20
        min_change_value = 11
        # 进行间歇性慢查询设计
        effect_inject_sqls = ["select * from orders where O_CARRIER_ID > 10;"]
        effect_inject_flag = 1
        workload_inject_flag = 1
    elif fault_type == 18:
        # 进行慢查询和锁冲突设计
        lock_sqls, slow_query = lock_slow_query()
        effect_inject_sqls = slow_query
        cause_inject_sqls = lock_sqls
        effect_inject_flag = 1
        cause_inject_flag = 1
    elif fault_type == 19:   # 整体工作负载流量增加
        traffic_inject_flag = 1
    else:
        # 注入正常sql语句，用于调节参数
        effect_inject_sqls = ["select * from orders where O_CARRIER_ID = 6;"]
        effect_inject_flag = 1

    the_index = 0
    if effect_inject_flag == 1:
        # 随机选择一条
        random_index = random.randint(0, len(effect_inject_sqls)-1)
        the_index = random_index
        effect_inject_sql = effect_inject_sqls[random_index]
        print("effect inject sql:", effect_inject_sql)
        # 打开文件并获取文件对象
        file = open(effect_path, "w")
        file.write(effect_inject_sql + "\n")
        file.close()

    if cause_inject_flag == 1:
        cause_inject_sql = cause_inject_sqls[the_index]
        print("cause inject sql:", cause_inject_sql)
        # 打开文件并获取文件对象
        file = open(cause_path, "w")
        file.write(cause_inject_sql + "\n")
        file.close()

    while (datetime.now() - start).seconds <= duration:
        print("time", (datetime.now() - start).seconds)
        # 触发为 因 的故障注入（设定为触发为 果 的故障注入时刻的0.5，即一般情况下为第30秒）
        if cause_inject_time < (datetime.now() - start).seconds < fault_inject_time:
            # 判断是否触发工作负载变化（即一般情况下为第30秒至第90秒）
            if workload_inject_flag == 1:
                global MIN_CARRIER_ID
                MIN_CARRIER_ID = min_change_value
                global MAX_CARRIER_ID
                MAX_CARRIER_ID = max_change_value
            # 注入为 因 的sql
            if cause_inject_flag == 1 and total_fault_count2 < total_cause_fault_number:   # count
                # 注入故障的客户端
                if command_inject_flag == 1:
                    future = pool.submit(execute_command, cause_inject_sql)    # 服务器执行故障命令行
                else:
                    future = pool.submit(Fault_Session, cause_inject_sql)      # 客户端执行故障sql
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

        # 在注入 因 产生变化之前，同样进行 果 故障sql的注入用于对照
        if int(fault_inject_time * 0.2) < (datetime.now() - start).seconds < int(fault_inject_time * 0.4) and total_fault_count1 < total_effect_fault_number:
            # 此时需因果故障sql同时存在方进行对照注入
            if effect_inject_flag == 1 and cause_inject_flag == 1:
                # 注入故障的客户端
                if command_inject_flag == 1:
                    future = pool.submit(execute_command, effect_inject_sql)  # 服务器执行故障命令行
                else:
                    future = pool.submit(Fault_Session, effect_inject_sql)  # 客户端执行故障sql
                # 获取线程返回的结果
                pool_results.append(future)
                # 每次注入故障sql后数量增加1
                total_fault_count1 = total_fault_count1 + 1
                print("effect_fault_count_before:", total_fault_count1)

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
        if (datetime.now() - start).seconds > fault_inject_time and total_fault_count < total_effect_fault_number:
            if effect_inject_flag == 1:
                # 注入故障的客户端
                if command_inject_flag == 1:
                    future = pool.submit(execute_command, effect_inject_sql)  # 服务器执行故障命令行
                else:
                    future = pool.submit(Fault_Session, effect_inject_sql)  # 客户端执行故障sql
                # 获取线程返回的结果
                pool_results.append(future)
                # 每次注入故障sql后数量增加1
                total_fault_count = total_fault_count + 1
                print("effect_fault_count:", total_fault_count)

            if traffic_inject_flag == 1:
                # 通过参数提高工作负载的流量
                sleep_time = 0.044
                max_thread = 500
                print("----traffic------")
                # 每次注入故障sql后数量增加1
                total_fault_count = total_fault_count + 1
                print("effect_fault_count:", total_fault_count)

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


def main(argv):
    duration = 0
    fault_inject_time = 0
    sleep_time = 0
    fault_type = 0
    total_cause_fault_number = 0
    total_effect_fault_number = 0
    root_cause_path = ""
    cause_inject_time = 30
    try:
        # 处理传入的参数内容
        opts, args = getopt.getopt(argv, "hd:d:t:i:s:c:e:p:x:")
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-d':
            duration = int(arg)
        if opt == '-t':
            fault_inject_time = int(arg)
        if opt == '-i':
            fault_type = int(arg)
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


    execute(duration, fault_inject_time, sleep_time, fault_type, total_cause_fault_number, total_effect_fault_number, cause_inject_time, root_cause_path)


if __name__ == "__main__":
    # -d 正常tpcc负载持续运行时间(s)
    # -t 注入果关系故障的时刻(一般设置为第60s)
    # -i 故障注入的类型
    # -s 每个任务提交之后的休眠时间(s)
    # -c 故障注入任务的数量（为因关系的故障）
    # -e 故障注入任务的数量（为果关系的故障 / 单一类型故障）
    # -p 存储故障根因的路径
    # -x 注入因关系故障的时刻（一般设置为第30s）
    args = ['-d', '100000', '-t', '60', '-i', '5', '-s', '0.044', '-c', '1', '-e', '10', '-p', '/root/mysqlrc/knowledge_graph/monitor/prometheus/9-29-9-34-13', '-x', 30]
    main(args)
    # main(sys.argv[1:])