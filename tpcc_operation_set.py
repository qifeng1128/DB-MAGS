import random

tables = ["new_orders", "orders", "order_line", "customer", "warehouse", "district", "item", "stock", "history"]

indexs = [["no_o_id", "no_d_id", "no_w_id"], ["o_id", "o_d_id", "o_w_id"], ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number"],
          ["c_id", "c_d_id", "c_w_id"], ["w_id"], ["d_id", "d_w_id"], ["i_id"], ["s_i_id", "s_w_id", "s_order_cnt"],
          ["h_c_w_id", "h_w_id"]]

max_values = [[15663, 10, 100], [15663, 10, 100], [15663, 10, 100, 15],
              [3000, 10, 100], [100], [10, 100], [100000], [100000, 100, 114],
              [100, 100]]

without_tables = ["orders", "order_line", "customer", "district", "item", "stock", "history"]

without_indexs = [["o_c_id", "o_carrier_id", "o_ol_cnt", "o_all_local"], ["ol_i_id", "ol_quantity"],
                  ["c_credit_lim", "c_payment_cnt", "c_delivery_cnt"], ["d_next_o_id"],
                  ["i_im_id"], ["s_quantity", "s_order_cnt", "s_remote_cnt"],
                  ["h_c_id", "h_c_d_id", "h_d_id"]]

without_max_values = [[3000, 10, 15, 1], [100000, 10], [50000, 32, 14], [17617], [10000], [100, 114, 4], [3000, 10, 10]]

too_much_tables = ["table_200000_undef_undef", "table_245215_undef_undef", "table_300000_undef_undef", "table_367339_undef_undef",
                   "table_400000_undef_undef", "table_433323_undef_undef"]

too_much_indexs = [["col_int_key_signed", "col_tinyint_key_signed", "col_smallint_key_signed", "col_bigint_key_signed"],
                   ["col_int_key_signed", "col_tinyint_key_signed", "col_smallint_key_signed", "col_bigint_key_signed"],
                   ["col_int_key_signed", "col_tinyint_key_signed", "col_smallint_key_signed", "col_bigint_key_signed"],
                   ["col_int_key_signed", "col_tinyint_key_signed", "col_smallint_key_signed", "col_bigint_key_signed"],
                   ["col_int_key_signed", "col_tinyint_key_signed", "col_smallint_key_signed", "col_bigint_key_signed"],
                   ["col_int_key_signed", "col_tinyint_key_signed", "col_smallint_key_signed", "col_bigint_key_signed"]]

too_much_max_values = [[32767, 127, 32767, 32766],
                       [32767, 127, 32767, 32767],
                       [32767, 127, 32767, 32766],
                       [32767, 127, 32767, 32766],
                       [32767, 127, 32767, 32767],
                       [32767, 127, 32767, 32766]]

slow_sql = ["select count(*) from new_orders where no_d_id = 1;",
            "select count(*) from orders where o_d_id = 1;",
            "select count(*) from order_line where ol_d_id = 1;",
            "select count(*) from customer where c_d_id = 1;",
            "select count(*) from warehouse where w_id = 1;",
            "select count(*) from district where d_id = 1;",
            "select count(*) from item where i_id = 1;",
            "select count(*) from stock where s_w_id = 1;",
            "select count(*) from history where h_c_d_id = 1;"]

fault_inject_type = ["table_lock", "meta_data_lock", "record_lock", "implicit_conversion", "query_with_too_much_join", "order_by", "group_by", "query_whole_table"]


generated_sql = ["update stock set stock.s_ytd = 35 where stock.s_dist_03 <= 'WQQwApz74dCcoNSadaU8qPXh' and stock.s_dist_03 = 'qpSqtGEhBdzmPl9uLff0G7aa';",
"update customer set customer.c_discount = 0.23 where customer.c_first < 'S9MLo9fL' and customer.c_since = '2023-08-29 21:30:45';",
"update stock set stock.s_data = 'rQXYVnGuArRZwIxySRmNlqO15INYWtILIntnYAfi' where stock.s_data > '0keaoQcvSwfJwhywQXJXVrWWjhvRnzFnjuLXKyRks63nlzj';",
"update item set item.i_data = 'i2qZSt6j6VCfaULDkexRoriginala42I6trX' where item.i_name != 'Qn891eTcCSfY0ewE';",
"update orders set orders.o_carrier_id = 4 where orders.o_ol_cnt <= 7 and orders.o_all_local >= 1;",
"update stock set stock.s_dist_08 = 'ggT2I5JObMIcSxVa1eMvDPNI' where stock.s_dist_03 > 'WmQF9tqqx11jiGZQ9a9W6SP4' and stock.s_dist_02 >= 'r9QxgBSkHgJ6iBZeWodQFO89';",
"update order_line set order_line.ol_quantity = 5 where order_line.ol_number < 6;",
"update stock set stock.s_dist_06 = 'Z9nSCiAXQNhhdzZ92AoJKKQr' where stock.s_data > 'k9Hd1rPWDcHMBIqSqOOvsUsGMZmvLOI5' and stock.s_dist_06 > 'NIsZTaLU5Og42o8dd8UQpliY' and stock.s_dist_02 < 'G3BK9o0k6BY0JxBeK5RlHDuD';",
"update orders set orders.o_ol_cnt = 14 where orders.o_entry_d = '2023-08-29 21:30:45';",
"update customer set customer.c_credit = 'BC' where customer.c_ytd_payment != 188.11;",
"update stock set stock.s_dist_03 = 'GfHjtvBhq7uFIjEwvg85Wynd' where stock.s_dist_04 != 'DLvPjHLv1bkSBftJNK8XYSmA' and stock.s_remote_cnt >= 1 and stock.s_quantity < 54;",
"update stock set stock.s_dist_04 = '28uub3YoSCAREykG8UOh1Th4' where stock.s_dist_03 < '7f4kOMlfvXE1SLvFxCfK0wWo';",
"update order_line set order_line.ol_amount = 0.00 where order_line.ol_amount > 0.00 and order_line.ol_delivery_d > '2023-08-29 21:30:45' and order_line.ol_d_id < 2;",
"update stock set stock.s_dist_07 = 'zdhp2lCutFsGhU9K1lJqWI9r' where stock.s_order_cnt >= 7;",
"update customer set customer.c_credit_lim = 50000 where customer.c_discount <= 0.34;",
"update order_line set order_line.ol_amount = 0.00 where order_line.ol_supply_w_id > 1;",
"update customer set customer.c_middle = 'OE' where customer.c_street_2 != 'avGURANFRi4mFQ' and customer.c_payment_cnt <= 1;",
"update orders set orders.o_ol_cnt = 14 where orders.o_carrier_id > 4;",
"update stock set stock.s_dist_02 = 'fMFInehw5xgnjdu8S7gq2Rz9' where stock.s_quantity > 37 and stock.s_dist_03 <= '0YoNBINJN5tIY8AcXDsDd4U9' and stock.s_dist_03 < 'jMKCLkn6JiMlWgoj6PC8PFqn' and stock.s_ytd != 41 and stock.s_dist_07 != 'AJflyqhXTiehO1NvqKQaaS5P';",
"update stock set stock.s_ytd = 11 where stock.s_quantity < 69;"]

# generated_sql = ["select customer.c_ytd_payment from customer where customer.c_last != 'ABLEPRIBAR';",
# "select history.h_c_d_id from history;",
# "select order_line.ol_w_id from order_line;",
# "select orders.o_all_local from orders where orders.o_ol_cnt >= 6;",
# "select item.i_data, count(item.i_name) from item group by item.i_data;",
# "select customer.c_last from customer order by customer.c_last ASC;",
# "select order_line.ol_dist_info from order_line order by order_line.ol_dist_info DESC;",
# "select order_line.ol_i_id from order_line;",
# "select stock.s_dist_09 from stock;",
# "select item.i_im_id from item where item.i_name != 'WYyLZ3o6XNiRYV' and item.i_id > 66;"]

def get_sql(index):
    return generated_sql[index]



# 锁冲突导致的慢查询
def lock_slow_query():
    lock_sqls = []
    slow_query = []
    for i in range(len(tables)):
        # 锁冲突语句
        one_sql = "Lock TABLES " + tables[i] + " WRITE;"
        lock_sqls.append(one_sql)
        # 慢查询语句
        slow_query.append(slow_sql[i])
    return lock_sqls, slow_query

# 表锁
def table_lock_sqls():
    sqls = []
    # 选取表格增加表写锁
    for i in range(len(tables)):
        one_sql = "Lock TABLES " + tables[i] + " WRITE;"
        sqls.append(one_sql)
    return sqls

# MDL锁
def meta_data_sqls():
    sqls = []
    # 选取表格增加元数据锁
    for i in range(len(tables)):
        one_sql = "ALTER TABLE " + tables[i] + " ADD meta_data char(2);"
        sqls.append(one_sql)
    return sqls

# 行锁
def record_lock_sqls(ratio):
    sqls = []
    # 选取表格和字段增加元数据锁
    for i in range(len(tables)):
        for j in range(len(indexs[i])):
            one_sql = "Update " + tables[i] + " set " + indexs[i][j] + " = " + indexs[i][j] + " + 0 where " + indexs[i][j] + " > " + str(int(ratio * max_values[i][j])) + ";"
            sqls.append(one_sql)
    return sqls

# def record_lock_sqls_insert(ratio):
#     # 对插入语句进行处理（删除多余的字段内容）
#     final_insert_sql = "insert into warehouse values "
#     f = open("Sql_Data/insert/warehouse_1000")
#     data = f.read()
#     data_list = data.split("(")
#     for i in range(1, len(data_list)):
#         position = data_list[i].index(',')
#         tempt = data_list[i][position+1:]
#         final_insert_sql = final_insert_sql + "(" + str(tempt)
#
#     print(final_insert_sql)

# 数据表备份（因果关系 考虑tpcc负载中进行写操作的表）
def mysql_dump_cause():
    sqls =  [f"mysqldump -u root -p'Meituan312' tpcc10_test customer > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test district > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test history > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test new_orders > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test order_line > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test orders > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test stock > test.sql",
            f"mysqldump -u root -p'Meituan312' tpcc10_test warehouse > test.sql"]
    return sqls

# 数据表备份（伴随关系 考虑tpcc负载中不进行写操作的表，防止锁冲突异常的生成）
def mysql_dump():
    sqls = [f"mysqldump -u root -p'Meituan312' tpcc10_test item > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_200000_undef_undef > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_200000_undef_undef_withoutkey > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_245215_undef_undef > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_245215_undef_undef_withoutkey > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_300000_undef_undef > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_300000_undef_undef_withoutkey > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_367339_undef_undef > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_367339_undef_undef_withoutkey > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_400000_undef_undef > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_400000_undef_undef_withoutkey > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_433323_undef_undef > test.sql",
            f"mysqldump -u root -p'Meituan312' order_by_test table_433323_undef_undef_withoutkey > test.sql"]
    return sqls

# 资源瓶颈
def cpu_bottle():
    sqls = [f"/root/ChaosBlade/chaosblade-0.3.0/blade create cpu fullload"]
    return sqls

def io_bottle():
    sqls = [f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk burn --read --path /home",
            f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk burn --write --path /home",
            f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk burn --read --write"]
    return sqls

def disk_bottle():
    sqls = [f"/root/ChaosBlade/chaosblade-0.3.0/blade create disk fill --path /home --size 40000"]
    return sqls

def mem_bottle():
    sqls = [f"/root/ChaosBlade/chaosblade-0.3.0/blade c mem load --mem-percent 70"]
    return sqls

def net_bottle():
    sqls = [f"/root/ChaosBlade/chaosblade-0.3.0/blade create network delay --time 5000 --offset 1000 --interface eth0 --local-port 3306",
            f"/root/ChaosBlade/chaosblade-0.3.0/blade create network loss --percent 100 --interface eth0 --local-port 3306"]
    return sqls

# 缺失索引
def missing_index():
    sqls = []
    for i in range(len(without_tables)):
        for j in range(len(without_indexs[i])):
            max_value = without_max_values[i][j]
            tempt = random.randint(0, max_value)
            one_sql = "select count(*) from " + without_tables[i] + " where " + without_indexs[i][j] + " > " + str(tempt) + ";"
            sqls.append(one_sql)
    return sqls

# 索引过多
def too_much_index():
    sqls = []
    for i in range(len(too_much_tables)):
        for j in range(len(too_much_indexs[i])):
            max_value = too_much_max_values[i][j]
            tempt = random.randint(0, int(0.5 * max_value))
            one_sql = "Update order_by_test." + too_much_tables[i] + " set " + too_much_indexs[i][j] + " = " + too_much_indexs[i][j] + " - 1 where " + too_much_indexs[i][j] + " > " + str(tempt) + ";"
            sqls.append(one_sql)
    return sqls

# 隐式转换
def implicit_conversion():
    return ["select * from implicit_join_without_index_table,history where implicit_join_without_index_table.h_c_id = history.h_c_id limit 20000;"]

# join字段过多
def query_with_too_much_join():
    f = open("/root/mysqlrc/fault_injection/Sql_Data/join/output.rand.sql")
    data = f.readlines()

    for i in range(len(data)):
        one_sql = data[i].replace("\n", "")
        one_sql_list = one_sql.split(" ")
        the_string = ""
        for j in range(len(one_sql_list)):
            the_string = the_string + one_sql_list[j]
            the_string = the_string + " "
            if j == 3 or j == 5:
                the_string = the_string + "order_by_test."

        data[i] = the_string

    return data

# order by使用磁盘临时表
def order_by():
    f = open("/root/mysqlrc/fault_injection/Sql_Data/order_by/output.rand.sql")
    data = f.readlines()

    for i in range(len(data)):
        one_sql = data[i].replace("\n", "")
        one_sql_list = one_sql.split(" ")
        the_string = ""
        for j in range(len(one_sql_list)):
            the_string = the_string + one_sql_list[j]
            the_string = the_string + " "
            if j == 2:
                the_string = the_string + "order_by_test."

        data[i] = the_string

    return data

# group by使用磁盘临时表
def group_by():
    f = open("/root/mysqlrc/fault_injection/Sql_Data/group_by/output.rand.sql")
    data = f.readlines()

    for i in range(len(data)):
        one_sql = data[i].replace("\n", "")
        one_sql_list = one_sql.split(" ")
        the_string = ""
        for j in range(len(one_sql_list)):
            the_string = the_string + one_sql_list[j]
            the_string = the_string + " "
            if j == 2:
                the_string = the_string + "order_by_test."

        data[i] = the_string

    return data

# 查询结果过大（全表扫描）
def query_whole_table():

    return ["select * from order_by_test.table_200000_undef_undef limit 10000;",
            "select * from order_by_test.table_245215_undef_undef limit 10000;",
            "select * from order_by_test.table_300000_undef_undef limit 10000;",
            "select * from order_by_test.table_367339_undef_undef limit 10000;",
            "select * from order_by_test.table_400000_undef_undef limit 10000;",
            "select * from order_by_test.table_433323_undef_undef limit 10000;"]


