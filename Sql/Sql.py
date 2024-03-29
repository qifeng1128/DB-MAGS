import random

from Connection.Connection import Database
from Sql.generate_data import Columns


class Sql:
    #### 增
    def insert_sql(self, database_name, table_name, sql_num):
        '''
        批量构建插入语句
        :param database_name: 进行改操作的数据库名
        :param table_name: 进行改操作的表名
        :param sql_num: 批量生成的 sql 数量
        :return: 返回插入语句
        '''
        insert_sqls = []
        # 获取表格字段信息
        table_column = self.get_table_info(database_name, table_name)
        table_column_tuple = tuple(table_column)
        table_column_str = '(' + ','.join(table_column_tuple) + ')'
        # 根据字段类型构造值
        column = Columns(database_name, table_name)
        sql_data = column.call_row(sql_num)
        for i in range(sql_num):
            sql_data_tuple = tuple(sql_data[i])
            insert_sql = 'insert into ' + database_name + '.' + table_name + ' ' + str(table_column_str) + ' values ' + str(sql_data_tuple) + ';'
            insert_sqls.append(insert_sql)
        return insert_sqls

    #### 删
    def delete_sql(self, database_name, table_name, column_name, condition_bool, equal_bool, sql_num):
        '''
        批量构建删除语句
        :param dabase_name: 进行改操作的数据库名
        :param table_name: 进行改操作的表名
        :param column_name: 进行改操作的字段名
        :param condition_bool: 是否存在 where 条件判断
        :param equal_bool: where 判断条件中是否进行范围查询
        :param sql_num: 批量生成的 sql 数量
        :return: 返回删除语句和 where 字段语句
                 若 condition_bool=True，删除语句不包含分号，可两者进行构造
                 若 condition_bool=False，删除语句包含分号，可直接进行查询
        '''
        delete_sql = 'delete from ' + database_name + '.' + table_name
        # 是否增加where查询条件
        if not condition_bool:
            delete_sql = delete_sql + ';'
        where_sqls = self.where_sql(database_name, table_name, column_name, equal_bool, sql_num)
        return delete_sql, where_sqls

    #### 改
    def update_sql(self, database_name, table_name, condition_bool, column_name, equal_bool, sql_num):
        '''
        批量构建更改语句
        :param database_name: 进行改操作的数据库名
        :param table_name: 进行改操作的表名
        :param column_name: 进行改操作的字段名
        :param condition_bool: 是否存在 where 条件判断
        :param equal_bool: where 判断条件中是否进行范围查询
        :param sql_num: 批量生成的 sql 数量
        :return: 返回更改语句和 where 字段语句
                 若 condition_bool=True，更改语句不包含分号，可两者进行构造
                 若 condition_bool=False，更改语句包含分号，可直接进行查询
        '''
        update_sqls = []
        where_sqls = self.where_sql(database_name, table_name, column_name, equal_bool, sql_num)
        # 根据字段类型构造值
        column = Columns(database_name, table_name)
        sql_data = column.call_row(sql_num)
        for k in range(sql_num):
            # 获取表格字段信息
            table_column = self.get_table_info(database_name, table_name)
            # 更改的字段名
            attri = column_name
            attri_index = table_column.index(attri)
            # 更改的字段值
            update_value = sql_data[k][attri_index]
            # 根据字段类型构造值
            if isinstance(update_value, str):
                update_sql = "update " + database_name + "." + table_name + " set " + attri + "='" + update_value + "'"
            else:
                update_sql = 'update ' + database_name + '.' + table_name + ' set ' + attri + '=' + str(update_value)
            # 是否增加where查询条件
            if not condition_bool:
                update_sql = update_sql + ';'
            update_sqls.append(update_sql)
        # 根据数据库和表名获取where查询字段
        if not condition_bool:
            where_sqls = []
        return update_sqls, where_sqls

    #### 查
    def select_sql(self, database_name, table_name, condition_bool, column_name, equal_bool, sql_num):
        '''
        批量构建查找语句
        :param database_name: 进行查操作的数据库名
        :param table_name: 进行查操作的表名
        :param column_name: 进行查操作的字段名
        :param condition_bool: 是否存在 where 条件判断
        :param equal_bool: where 判断条件中是否进行范围查询
        :param sql_num: 批量生成的 sql 数量
        :return: 返回查询语句和 where 字段语句
                 若 condition_bool=True，查询语句不包含分号，可两者进行构造
                 若 condition_bool=False，查询语句包含分号，可直接进行查询
        '''
        select_sqls = []
        where_sqls = []
        for k in range(sql_num):
            select_sql = ''
            # 获取表格字段信息
            table_column = self.get_table_info(database_name, table_name)
            # 随机选取展示的字段
            attri = ''
            attri_num = random.randint(1, len(table_column))   # 随机选取获取字段的数量
            if attri_num == len(table_column):  # 选取全部字段
                attri = '*'
            else:
                for i in range(attri_num):   # 随机选取获取的字段内容
                    attri_index = random.randint(0,len(table_column) - 1)
                    if i == attri_num - 1:
                        attri = attri + table_column[attri_index] + ''
                    else:
                        attri = attri + table_column[attri_index] + ','
            select_sql = 'select ' + attri + ' from ' + database_name + '.' + table_name
            # 是否增加where查询条件
            if not condition_bool:
                select_sql = select_sql + ';'
            select_sqls.append(select_sql)
        # 根据数据库和表名获取where查询字段
        if condition_bool:
            where_sqls = self.where_sql(database_name, table_name, column_name, equal_bool, sql_num)
        return select_sqls, where_sqls


    def get_table_info(self, database_name, table_name):
        '''
        获取表格字段信息
        '''
        sql = 'desc ' + database_name + '.' + table_name + ';'
        db = Database()  # 这里必须在新的连接下发起事务，否则会在start_transaction处报错 非DatabaseError
        conn = db.conn
        cur = db.cursor
        cur.execute(sql)
        row_all = cur.fetchall()
        table_column = []
        for i in range(len(row_all)):
            #print(row_all[i])
            table_column.append(row_all[i][0])
        return table_column

    def where_sql(self, database_name, table_name, column_name, equal_bool, sql_num):
        '''
        随机获取判断条件中对应字段的值
        '''
        count_sql = 'select count(*) from ' + database_name + '.' + table_name + ';'
        db = Database()  # 这里必须在新的连接下发起事务，否则会在start_transaction处报错 非DatabaseError
        conn = db.conn
        cur = db.cursor
        cur.execute(count_sql)
        row_all = cur.fetchall()
        total_number = row_all[0][0]
        where_sqls = []
        for j in range(sql_num):
            # 获取where条件中的字段名
            where_attri = column_name
            # 随机选取字段对应的一个值
            position = random.randint(0, total_number - sql_num)
            value_sql = 'select ' + where_attri + ' from ' + database_name + '.' + table_name + ' limit ' + str(position) + ',1' + ';'
            cur.execute(value_sql)
            row_all = cur.fetchall()
            if isinstance(row_all[0][0],str):
                if equal_bool:
                    where_sql = "where " + where_attri + " = '" + row_all[0][0] + "'"
                else:
                    where_sql = "where " + where_attri + " > '" + row_all[0][0] + "'"
            else:
                if equal_bool:
                    where_sql = 'where ' + where_attri + ' = ' + str(row_all[0][0])
                else:
                    where_sql = 'where ' + where_attri + ' > ' + str(row_all[0][0])
            where_sqls.append(where_sql)
        return where_sqls

    """ 隐式类型转换 """

    def implicit_conversion_type(self):
        '''
        构建字段类型不同的隐式转换sql
        （char字段 = int字段）
        '''
        db = Database()  # 这里必须在新的连接下发起事务，否则会在start_transaction处报错 非DatabaseError
        conn = db.conn
        cur = db.cursor
        # 在tpcc customer表中的c_zip字段进行随机查找
        cur.execute("select c_zip from customer where c_zip like '1%' limit 10")  # 选取10个字段进行查找
        row_all = cur.fetchall()
        int_row_all = "("
        char_row_all = "("
        for i in range(len(row_all)):
            if i == len(row_all) - 1:
                int_row_all = int_row_all + row_all[i][0] + ')'
                char_row_all = char_row_all + '"' + row_all[i][0] + '")'
            else:
                int_row_all = int_row_all + row_all[i][0] + ','
                char_row_all = char_row_all + '"' + row_all[i][0] + '",'
        # 构建索引未失效sql
        sql_with_index = "select * from customer where c_zip in " + char_row_all
        # 构建索引失效sql
        sql_without_index = "select * from customer where c_zip in " + int_row_all
        return sql_with_index, sql_without_index

    def implicit_conversion_join(self):
        '''
        构建关联字段类型不同的隐式转换sql
        （join on 表 a.char字段 = 表 b.int字段）
        '''
        # 从tpcc history表中抽取部分数据构造h_c_id字段为char类型和int类型的join表
        # 构建索引未失效sql（int类型=int类型）
        sql_with_index = "select * from implicit_join_without_index_table1,history where implicit_join_without_index_table1.h_c_id = history.h_c_id limit 10000;"
        # 构建索引失效sql（char类型=int类型）
        sql_without_index = "select * from implicit_join_with_index_table1,history where implicit_join_with_index_table1.h_c_id = history.h_c_id limit 10000;"
        return sql_with_index, sql_without_index

    def implicit_conversion_charset(self):
        '''
        char类型字段字符集不同的隐式转换sql
        （utf8字符类型 = utf8mb4字符类型）
        '''
        # 从tpcc history表中抽取部分数据构造h_data字段为utf8字符类型和utf8mb4字符类型的join表
        # 构建索引未失效sql（utf8类型=utf8类型）
        sql_with_index = "select * from implicit_charset_without_index_table1,history where implicit_charset_without_index_table1.h_data = history.h_data limit 10;"
        # 构建索引失效sql（utf8类型=utfmb4类型）
        sql_without_index = "select * from implicit_charset_with_index_table1,history where implicit_charset_with_index_table1.h_data = history.h_data limit 10;"
        return sql_with_index, sql_without_index

    def implicit_conversion_validation(self):
        '''
        校验规则不一致的隐式转换sql
        （utf8_bin = utf8_general_ci）
        '''
        # 从tpcc history表中抽取部分数据构造h_data字段为utf8_bin校验规则和utf8_general_ci校验规则的join表
        # 构建索引未失效sql（utf8_general_ci校验规则=utf8_general_ci校验规则）
        sql_with_index = "select * from implicit_validation_with_index_table1,history where implicit_validation_with_index_table1.h_data = history.h_data limit 100000;"
        # 构建索引失效sql（utf8_bin校验规则=utf8_general_ci校验规则）
        sql_without_index = "select * from implicit_validation_without_index_table1,history where implicit_validation_without_index_table1.h_data = history.h_data limit 100000;"
        return sql_with_index, sql_without_index

    def query_with_or(self):
        '''
        查询条件包含or
        （查询 history 表中 h_c_id 字段为 2 或 8 的结果，使用 union all 来构造索引未失效的 sql）
        '''
        # 构造索引失效的sql
        sql_without_index = " select * from history where h_c_id = 2 union all select * from history where h_c_w_id = 8;"
        # 构造索引未失效的sql
        sql_with_index = " select * from history where h_c_id = 2 or h_c_w_id = 8;"

    """ like查询 """

    def sql_with_like(self):
        '''
        like查询
        （查询条件最左以通配符%开头，使用
        '''
