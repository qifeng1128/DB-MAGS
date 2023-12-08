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

from tempt import table_lock_sqls, meta_data_sqls, record_lock_sqls, missing_index, too_much_index, implicit_conversion, \
    query_with_too_much_join, order_by, group_by, query_whole_table, lock_slow_query, mysql_dump, cpu_bottle, io_bottle, disk_bottle, \
    mem_bottle, net_bottle

from Connection.Connection import Database

#  Used to generate stock level transactions
MIN_STOCK_LEVEL_THRESHOLD = 10
MAX_STOCK_LEVEL_THRESHOLD = 20

DISTRICTS_PER_WAREHOUSE = 10
CUSTOMERS_PER_DISTRICT = 3000
NUM_ITEMS = 100000

MIN_OL_CNT = 5
MAX_OL_CNT = 15

#  Used to generate new order transactions
MAX_OL_QUANTITY = 10

#  New order constants
INITIAL_NEW_ORDERS_PER_DISTRICT = 900

#  Order constants
MIN_CARRIER_ID = 0          #  修改tpcc工作负载
MAX_CARRIER_ID = 10          #  修改tpcc工作负载

#  Used to generate payment transactions
MIN_PAYMENT = 1.0
MAX_PAYMENT = 5000.0

#  HACK: This is not strictly correct, but it works
NULL_CARRIER_ID = 0

#  Indicates "brand" items and stock in i_data and s_data.
ORIGINAL_STRING = "ORIGINAL"

BAD_CREDIT = "BC"

MIN_C_DATA = 300
MAX_C_DATA = 500

# Transaction Types
def enum(*sequential, **named):
    enums = dict(map(lambda x: (x, x), sequential))
    # dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

TransactionTypes = enum(
    "DELIVERY",
    "NEW_ORDER",
    "ORDER_STATUS",
    "PAYMENT",
    "STOCK_LEVEL",
)

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT NO_O_ID FROM new_orders WHERE NO_D_ID = %s AND NO_W_ID = %s AND NO_O_ID > -1 LIMIT 1",  #
        "deleteNewOrder": "DELETE FROM new_orders WHERE NO_D_ID = %s AND NO_W_ID = %s AND NO_O_ID = %s",
        # d_id, w_id, no_o_id
        "getCId": "SELECT O_C_ID FROM orders WHERE O_ID = %s AND O_D_ID = %s AND O_W_ID = %s",  # no_o_id, d_id, w_id
        "updateOrders": "UPDATE orders SET O_CARRIER_ID = %s WHERE O_ID = %s AND O_D_ID = %s AND O_W_ID = %s",
        # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE order_line SET OL_DELIVERY_D = %s WHERE OL_O_ID = %s AND OL_D_ID = %s AND OL_W_ID = %s",
        # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM order_line WHERE OL_O_ID = %s AND OL_D_ID = %s AND OL_W_ID = %s",
        # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE customer SET C_BALANCE = C_BALANCE + %s WHERE C_ID = %s AND C_D_ID = %s AND C_W_ID = %s",
        # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM warehouse WHERE W_ID = %s",  # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM district WHERE D_ID = %s AND D_W_ID = %s",  # d_id, w_id
        "incrementNextOrderId": "UPDATE district SET D_NEXT_O_ID = %s WHERE D_ID = %s AND D_W_ID = %s",
        # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",
        # w_id, d_id, c_id
        "createOrder": "INSERT INTO orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO new_orders (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (%s, %s, %s)",  # o_id, d_id, w_id
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM item WHERE I_ID = %s",  # ol_i_id
        "getStockInfo1": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_",
        "getStockInfo2": " FROM stock WHERE S_I_ID = %s AND S_W_ID = %s",
        # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE stock SET S_QUANTITY = %s, S_YTD = %s, S_ORDER_CNT = %s, S_REMOTE_CNT = %s WHERE S_I_ID = %s AND S_W_ID = %s",
        # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO order_line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
    },

    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",
        # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_LAST = %s ORDER BY C_FIRST",
        # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM orders WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s ORDER BY O_ID DESC LIMIT 1",
        # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s",
        # w_id, d_id, o_id
    },

    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM warehouse WHERE W_ID = %s",
        # w_id
        "updateWarehouseBalance": "UPDATE warehouse SET W_YTD = W_YTD + %s WHERE W_ID = %s",  # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM district WHERE D_W_ID = %s AND D_ID = %s",
        # w_id, d_id
        "updateDistrictBalance": "UPDATE district SET D_YTD = D_YTD + %s WHERE D_W_ID  = %s AND D_ID = %s",
        # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",
        # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM customer WHERE C_W_ID = %s AND C_D_ID = %s AND C_LAST = %s ORDER BY C_FIRST",
        # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE customer SET C_BALANCE = %s, C_YTD_PAYMENT = %s, C_PAYMENT_CNT = %s, C_DATA = %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",
        # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE customer SET C_BALANCE = %s, C_YTD_PAYMENT = %s, C_PAYMENT_CNT = %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s",
        # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO history VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    },

    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM district WHERE D_W_ID = %s AND D_ID = %s",
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM order_line, stock
            WHERE OL_W_ID = %s
              AND OL_D_ID = %s
              AND OL_O_ID < %s
              AND OL_O_ID >= %s
              AND S_W_ID = %s
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < %s
        """,
    },
}

class ScaleParameters:

    def __init__(self, items, warehouses, districtsPerWarehouse, customersPerDistrict, newOrdersPerDistrict):
        assert 1 <= items and items <= NUM_ITEMS
        self.items = items
        assert warehouses > 0
        self.warehouses = warehouses
        self.starting_warehouse = 1
        assert 1 <= districtsPerWarehouse and districtsPerWarehouse <= DISTRICTS_PER_WAREHOUSE
        self.districtsPerWarehouse = districtsPerWarehouse
        assert 1 <= customersPerDistrict and customersPerDistrict <= CUSTOMERS_PER_DISTRICT
        self.customersPerDistrict = customersPerDistrict
        assert 0 <= newOrdersPerDistrict and newOrdersPerDistrict <= CUSTOMERS_PER_DISTRICT
        assert newOrdersPerDistrict <= INITIAL_NEW_ORDERS_PER_DISTRICT
        self.newOrdersPerDistrict = newOrdersPerDistrict
        self.ending_warehouse = (self.warehouses + self.starting_warehouse - 1)

    ## DEF

    def __str__(self):
        out = "%d items\n" % self.items
        out += "%d warehouses\n" % self.warehouses
        out += "%d districts/warehouse\n" % self.districtsPerWarehouse
        out += "%d customers/district\n" % self.customersPerDistrict
        out += "%d initial new orders/district" % self.newOrdersPerDistrict
        return out
    ## DEF


## CLASS

def makeWithScaleFactor(warehouses, scaleFactor):
    assert scaleFactor >= 1.0

    items = int(NUM_ITEMS/scaleFactor)
    if items <= 0: items = 1
    districts = int(max(DISTRICTS_PER_WAREHOUSE, 1))
    customers = int(max(CUSTOMERS_PER_DISTRICT/scaleFactor, 1))
    newOrders = int(max(INITIAL_NEW_ORDERS_PER_DISTRICT/scaleFactor, 0))

    return ScaleParameters(items, warehouses, districts, customers, newOrders)
## DEF

## Create ScaleParameters
warehouses = 4
scalefactor = 1
scaleParameters = makeWithScaleFactor(warehouses, scalefactor)

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
        lock_ratio = 0.8         # todo:改为中间的任意值
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
    elif fault_type == 17:   # 工作负载的变化  todo
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

def executeTransaction(txn, params, conn):
    """Execute a transaction based on the given name"""

    if TransactionTypes.STOCK_LEVEL == txn:
        result = doStockLevel(params, conn)
    elif TransactionTypes.DELIVERY == txn:
        result = doDelivery(params, conn)
    elif TransactionTypes.ORDER_STATUS == txn:
        result = doOrderStatus(params, conn)
    elif TransactionTypes.PAYMENT == txn:
        result = doPayment(params, conn)
    elif TransactionTypes.NEW_ORDER == txn:
        result = doNewOrder(params, conn)
    else:
        assert False, "Unexpected TransactionType: " + txn
    return result

## ----------------------------------------------
## doDelivery
## ----------------------------------------------
def doDelivery(params, conn):
    cur = conn.cursor()

    q = TXN_QUERIES["DELIVERY"]

    w_id = params["w_id"]
    o_carrier_id = params["o_carrier_id"]
    ol_delivery_d = params["ol_delivery_d"]

    result = []
    for d_id in range(1, DISTRICTS_PER_WAREHOUSE + 1):
        cur.execute(q["getNewOrder"], [d_id, w_id])
        newOrder = cur.fetchone()
        if newOrder == None:
            ## No orders for this district: skip it. Note: This must be reported if > 1%
            continue
        assert len(newOrder) > 0
        no_o_id = newOrder[0]

        cur.execute(q["getCId"], [no_o_id, d_id, w_id])
        c_id = cur.fetchone()[0]

        cur.execute(q["sumOLAmount"], [no_o_id, d_id, w_id])
        ol_total = cur.fetchone()[0]

        cur.execute(q["deleteNewOrder"], [d_id, w_id, no_o_id])
        cur.execute(q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id])
        cur.execute(q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id])

        # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
        # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
        # them out
        # If there are no order lines, SUM returns null. There should always be order lines.
        assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
        assert ol_total > 0.0

        cur.execute(q["updateCustomer"], [ol_total, c_id, d_id, w_id])

        result.append((d_id, no_o_id))
    ## FOR

    # conn.commit()
    # return result

## ----------------------------------------------
## doNewOrder
## ----------------------------------------------
def doNewOrder(params, conn):
    cur = conn.cursor()

    q = TXN_QUERIES["NEW_ORDER"]

    w_id = params["w_id"]
    d_id = params["d_id"]
    c_id = params["c_id"]
    o_entry_d = params["o_entry_d"]
    i_ids = params["i_ids"]
    i_w_ids = params["i_w_ids"]
    i_qtys = params["i_qtys"]

    assert len(i_ids) > 0
    assert len(i_ids) == len(i_w_ids)
    assert len(i_ids) == len(i_qtys)

    all_local = True
    items = []
    for i in range(len(i_ids)):
        ## Determine if this is an all local order or not
        all_local = all_local and i_w_ids[i] == w_id
        cur.execute(q["getItemInfo"], [i_ids[i]])
        items.append(cur.fetchone())
    assert len(items) == len(i_ids)

    ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
    ## Note that this will happen with 1% of transactions on purpose.
    for item in items:
        if len(item) == 0:
            ## TODO Abort here!
            return
    ## FOR

    ## ----------------
    ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
    ## ----------------
    cur.execute(q["getWarehouseTaxRate"], [w_id])
    w_tax = cur.fetchone()[0]

    cur.execute(q["getDistrict"], [d_id, w_id])
    district_info = cur.fetchone()
    d_tax = district_info[0]
    d_next_o_id = district_info[1]

    cur.execute(q["getCustomer"], [w_id, d_id, c_id])
    customer_info = cur.fetchone()
    c_discount = customer_info[0]

    ## ----------------
    ## Insert Order Information
    ## ----------------
    ol_cnt = len(i_ids)
    o_carrier_id = NULL_CARRIER_ID

    cur.execute(q["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
    cur.execute(q["createOrder"],
                        [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
    cur.execute(q["createNewOrder"], [d_next_o_id, d_id, w_id])

    ## ----------------
    ## Insert Order Item Information
    ## ----------------
    item_data = []
    total = 0
    if d_id != 10:
        d_id = '0' + str(d_id)
    for i in range(len(i_ids)):
        ol_number = i + 1
        ol_supply_w_id = i_w_ids[i]
        ol_i_id = i_ids[i]
        ol_quantity = i_qtys[i]

        itemInfo = items[i]
        i_name = itemInfo[1]
        i_data = itemInfo[2]
        i_price = itemInfo[0]

        getStockInfo = q["getStockInfo1"] + str(d_id) + q["getStockInfo2"]
        cur.execute(getStockInfo, [ol_i_id, ol_supply_w_id])
        stockInfo = cur.fetchone()
        if len(stockInfo) == 0:
            logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
            continue
        s_quantity = stockInfo[0]
        s_ytd = stockInfo[2]
        s_order_cnt = stockInfo[3]
        s_remote_cnt = stockInfo[4]
        s_data = stockInfo[1]
        s_dist_xx = stockInfo[5]  # Fetches data from the s_dist_[d_id] column

        ## Update stock
        s_ytd += ol_quantity
        if s_quantity >= ol_quantity + 10:
            s_quantity = s_quantity - ol_quantity
        else:
            s_quantity = s_quantity + 91 - ol_quantity
        s_order_cnt += 1

        if ol_supply_w_id != w_id: s_remote_cnt += 1

        cur.execute(q["updateStock"],
                            [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id])

        if i_data.find(ORIGINAL_STRING) != -1 and s_data.find(ORIGINAL_STRING) != -1:
            brand_generic = 'B'
        else:
            brand_generic = 'G'

        ## Transaction profile states to use "ol_quantity * i_price"
        ol_amount = ol_quantity * i_price
        total += ol_amount

        cur.execute(q["createOrderLine"],
                            [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity,
                             ol_amount, s_dist_xx])

        ## Add the info to be returned
        item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
    ## FOR

    ## Commit!
    # conn.commit()

    ## Adjust the total for the discount
    # print "c_discount:", c_discount, type(c_discount)
    # print "w_tax:", w_tax, type(w_tax)
    # print "d_tax:", d_tax, type(d_tax)
    total *= (1 - c_discount) * (1 + w_tax + d_tax)

    ## Pack up values the client is missing (see TPC-C 2.4.3.5)
    misc = [(w_tax, d_tax, d_next_o_id, total)]

    # return [customer_info, misc, item_data]

## ----------------------------------------------
## doOrderStatus
## ----------------------------------------------
def doOrderStatus(params, conn):
    cur = conn.cursor()

    q = TXN_QUERIES["ORDER_STATUS"]

    w_id = params["w_id"]
    d_id = params["d_id"]
    c_id = params["c_id"]
    c_last = params["c_last"]

    assert w_id, pformat(params)
    assert d_id, pformat(params)

    if c_id != None:
        cur.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
        customer = cur.fetchone()
    else:
        # Get the midpoint customer's id
        cur.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
        all_customers = cur.fetchall()
        assert len(all_customers) > 0
        namecnt = len(all_customers)
        index = int((namecnt - 1) / 2)
        customer = all_customers[index]
        c_id = customer[0]
    assert len(customer) > 0
    assert c_id != None

    cur.execute(q["getLastOrder"], [w_id, d_id, c_id])
    order = cur.fetchone()

    if order:
        cur.execute(q["getOrderLines"], [w_id, d_id, order[0]])
        orderLines = cur.fetchall()
    else:
        orderLines = []

    # conn.commit()
    # return [customer, order, orderLines]

## ----------------------------------------------
## doPayment
## ----------------------------------------------
def doPayment(params, conn):
    cur = conn.cursor()

    q = TXN_QUERIES["PAYMENT"]

    w_id = params["w_id"]
    d_id = params["d_id"]
    h_amount = params["h_amount"]
    c_w_id = params["c_w_id"]
    c_d_id = params["c_d_id"]
    c_id = params["c_id"]
    c_last = params["c_last"]
    h_date = params["h_date"]

    if c_id != None:
        cur.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
        customer = cur.fetchone()
    else:
        # Get the midpoint customer's id
        cur.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
        all_customers = cur.fetchall()
        assert len(all_customers) > 0
        namecnt = len(all_customers)
        index = int((namecnt - 1) / 2)
        customer = all_customers[index]
        c_id = customer[0]
    assert len(customer) > 0
    c_balance = float(customer[14]) - h_amount
    c_ytd_payment = float(customer[15]) + h_amount
    c_payment_cnt = customer[16] + 1
    c_data = customer[17]

    cur.execute(q["getWarehouse"], [w_id])
    warehouse = cur.fetchone()

    cur.execute(q["getDistrict"], [w_id, d_id])
    district = cur.fetchone()

    cur.execute(q["updateWarehouseBalance"], [h_amount, w_id])
    cur.execute(q["updateDistrictBalance"], [h_amount, w_id, d_id])

    # Customer Credit Information
    if customer[11] == BAD_CREDIT:
        newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
        c_data = (newData + "|" + c_data)
        if len(c_data) > MAX_C_DATA: c_data = c_data[:MAX_C_DATA]
        cur.execute(q["updateBCCustomer"],
                            [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
    else:
        c_data = ""
        cur.execute(q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])

    # Concatenate w_name, four spaces, d_name
    h_data = "%s    %s" % (warehouse[0], district[0])
    # Create the history record
    cur.execute(q["insertHistory"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])

    # conn.commit()

    # TPC-C 2.5.3.3: Must display the following fields:
    # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
    # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
    # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
    # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
    # H_AMOUNT, and H_DATE.

    # Hand back all the warehouse, district, and customer data
    # return [warehouse, district, customer]

## ----------------------------------------------
## doStockLevel
## ----------------------------------------------
def doStockLevel(params, conn):
    cur = conn.cursor()

    q = TXN_QUERIES["STOCK_LEVEL"]

    w_id = params["w_id"]
    d_id = params["d_id"]
    threshold = params["threshold"]

    cur.execute(q["getOId"], [str(w_id), (d_id)])
    result = cur.fetchone()
    assert result
    o_id = result[0]

    cur.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
    result = cur.fetchone()

    # conn.commit()

    # return int(result[0])

# 随机选择一种事务类型并执行
def doOne():
    # 随机选择一种事务
    x = rand.number(1, 100)
    # print(x)
    if x <= 4:  ## 4%            4
        txn, params = (TransactionTypes.STOCK_LEVEL, generateStockLevelParams())
    elif x <= 4 + 4:  ## 4%             8
        txn, params = (TransactionTypes.DELIVERY, generateDeliveryParams())
    elif x <= 4 + 4 + 4:  ## 4%             12
        txn, params = (TransactionTypes.ORDER_STATUS, generateOrderStatusParams())
    elif x <= 43 + 4 + 4 + 4:  ## 43%                   55
        txn, params = (TransactionTypes.PAYMENT, generatePaymentParams())
    else:  ## 45%
        txn, params = (TransactionTypes.NEW_ORDER, generateNewOrderParams())

    return (txn, params)


def makeParameterDict(values, *args):
    return dict(map(lambda x: (x, values[x]), args))
## DEF


# 获取slev事务所需要的参数
def generateStockLevelParams():
    """Returns parameters for STOCK_LEVEL"""
    w_id = makeWarehouseId()
    d_id = makeDistrictId()
    threshold = rand.number(MIN_STOCK_LEVEL_THRESHOLD, MAX_STOCK_LEVEL_THRESHOLD)
    return makeParameterDict(locals(), "w_id", "d_id", "threshold")

def generateDeliveryParams():
    """Return parameters for DELIVERY"""
    w_id = makeWarehouseId()
    o_carrier_id = rand.number(MIN_CARRIER_ID, MAX_CARRIER_ID)
    ol_delivery_d = datetime.now()
    return makeParameterDict(locals(), "w_id", "o_carrier_id", "ol_delivery_d")

def generateOrderStatusParams():
    """Return parameters for ORDER_STATUS"""
    w_id = makeWarehouseId()
    d_id = makeDistrictId()
    c_last = None
    c_id = None

    ## 60%: order status by last name
    if rand.number(1, 100) <= 60:
        c_last = rand.makeRandomLastName(scaleParameters.customersPerDistrict)

    ## 40%: order status by id
    else:
        c_id = makeCustomerId()

    return makeParameterDict(locals(), "w_id", "d_id", "c_id", "c_last")

def generatePaymentParams():
    """Return parameters for PAYMENT"""
    x = rand.number(1, 100)
    y = rand.number(1, 100)

    w_id = makeWarehouseId()
    d_id = makeDistrictId()
    c_w_id = None
    c_d_id = None
    c_id = None
    c_last = None
    h_amount = rand.fixedPoint(2, MIN_PAYMENT, MAX_PAYMENT)
    h_date = datetime.now()

    ## 85%: paying through own warehouse (or there is only 1 warehouse)
    if scaleParameters.warehouses == 1 or x <= 85:
        c_w_id = w_id
        c_d_id = d_id
    ## 15%: paying through another warehouse:
    else:
        ## select in range [1, num_warehouses] excluding w_id
        c_w_id = rand.numberExcluding(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse, w_id)
        assert c_w_id != w_id
        c_d_id = makeDistrictId()

    ## 60%: payment by last name
    if y <= 60:
        c_last = rand.makeRandomLastName(scaleParameters.customersPerDistrict)
    ## 40%: payment by id
    else:
        assert y > 60
        c_id = makeCustomerId()

    return makeParameterDict(locals(), "w_id", "d_id", "h_amount", "c_w_id", "c_d_id", "c_id", "c_last", "h_date")

def generateNewOrderParams():
    """Return parameters for NEW_ORDER"""
    w_id = makeWarehouseId()
    d_id = makeDistrictId()
    c_id = makeCustomerId()
    ol_cnt = rand.number(MIN_OL_CNT, MAX_OL_CNT)
    o_entry_d = datetime.now()

    ## 1% of transactions roll back
    rollback = False       # FIXME rand.number(1, 100) == 1

    i_ids = [ ]
    i_w_ids = [ ]
    i_qtys = [ ]
    for i in range(0, ol_cnt):
        if rollback and i + 1 == ol_cnt:
            i_ids.append(scaleParameters.items + 1)
        else:
            i_id = makeItemId()
            while i_id in i_ids:
                i_id = makeItemId()
            i_ids.append(i_id)

        ## 1% of items are from a remote warehouse
        remote = (rand.number(1, 100) == 1)
        if scaleParameters.warehouses > 1 and remote:
            i_w_ids.append(rand.numberExcluding(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse, w_id))
        else:
            i_w_ids.append(w_id)

        i_qtys.append(rand.number(1, MAX_OL_QUANTITY))
    ## FOR

    threshold = rand.number(MIN_STOCK_LEVEL_THRESHOLD, MAX_STOCK_LEVEL_THRESHOLD)
    return makeParameterDict(locals(), "w_id", "d_id", "c_id", "o_entry_d", "i_ids", "i_w_ids", "i_qtys", "threshold")


def makeWarehouseId():
    w_id = rand.number(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse)
    assert(w_id >= scaleParameters.starting_warehouse), "Invalid W_ID: %d" % w_id
    assert(w_id <= scaleParameters.ending_warehouse), "Invalid W_ID: %d" % w_id
    return w_id
## DEF

def makeDistrictId():
    return rand.number(1, scaleParameters.districtsPerWarehouse)
## DEF

def makeCustomerId():
    return rand.NURand(1023, 1, scaleParameters.customersPerDistrict)
## DEF

def makeItemId():
    return rand.NURand(8191, 1, scaleParameters.items)
## DEF

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
    # args = ['-d', '100000', '-t', '60', '-i', '5', '-s', '0.044', '-c', '1', '-e', '10', '-p', '/root/mysqlrc/knowledge_graph/monitor/prometheus/9-29-9-34-13', '-x', 30]
    # main(args)
    main(sys.argv[1:])