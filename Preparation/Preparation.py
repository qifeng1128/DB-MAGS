import pymysql

from Transcation.Transcation import Transcation


class Preparation:
    def create_whole_table(self, original_table_name, new_table_name):
        '''
        复制数据库中原有表的全部内容至新表，用于故障注入
        '''
        tran = Transcation()
        conn, cur = tran.create_connection()
        sql_create = "CREATE TABLE " + new_table_name + " LIKE " + original_table_name + ";"
        cur.execute(sql_create)
        sql_insert = "INSERT INTO " + new_table_name + " SELECT * FROM " + original_table_name + ";"
        cur.execute(sql_insert)
        conn.commit()

    def get_insert_rows(self, select_sql):
        '''
        获取原表部分数据用于构建join的表
        '''
        tran = Transcation()
        conn, cur = tran.create_connection()
        cur.execute(select_sql)
        row_all = cur.fetchall()
        # 获取插入新表内容
        insert_value = []
        for one_row in row_all:
            one_tuple = []
            for one_column in one_row:
                one_tuple.append(str(one_column))
            one_tuple = tuple(one_tuple)
            insert_value.append(one_tuple)
        return insert_value

    def create_part_table(self, create_table_sql, select_sqls, insert_sql):
        '''
        复制数据库中原有表的部分内容至新表，用于故障注入
        '''
        tran = Transcation()
        conn, cur = tran.create_connection()
        cur.execute(create_table_sql)
        for select_sql in select_sqls:
            insert_value = self.get_insert_rows(select_sql)
            effect_row = cur.executemany(insert_sql, insert_value)
            conn.commit()


def prepare_join_table(pre,select_sqls):
    '''
    构建用于关联字段类型不一致的隐式转换的join表
    '''
    # 构建用于隐式转换join的表(索引失效）
    create_table_sql = "CREATE TABLE implicit_join_without_index_table (h_c_id VARCHAR(255), h_data VARCHAR(255), index index_cid(h_c_id))"
    insert_sql = "insert into implicit_join_without_index_table (h_c_id,h_data) values(%s,%s)"
    pre.create_part_table(create_table_sql, select_sqls, insert_sql)

    # 构建用于隐式转换join的表(索引有效）
    create_table_sql = "CREATE TABLE implicit_join_with_index_table (h_c_id SMALLINT(6), h_data VARCHAR(255), index index_cid(h_c_id))"
    insert_sql = "insert into implicit_join_with_index_table (h_c_id,h_data) values(%s,%s)"
    pre.create_part_table(create_table_sql, select_sqls, insert_sql)

def prepare_charset_table(pre,select_sqls):
    '''
    构建用于char类型字段不一致的隐式转换的join表
    '''
    # 构建用于隐式转换join的表(索引失效）
    create_table_sql = "CREATE TABLE implicit_charset_without_index_table (h_c_id VARCHAR(255), h_data VARCHAR(255), index index_cid(h_data)) ENGINE=InnoDB DEFAULT CHARSET=gbk"
    insert_sql = "insert into implicit_charset_without_index_table (h_c_id,h_data) values(%s,%s)"
    pre.create_part_table(create_table_sql, select_sqls, insert_sql)

    # 构建用于隐式转换join的表(索引有效）
    create_table_sql = "CREATE TABLE implicit_charset_with_index_table (h_c_id VARCHAR(255), h_data VARCHAR(255), index index_cid(h_data)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    insert_sql = "insert into implicit_charset_with_index_table (h_c_id,h_data) values(%s,%s)"
    pre.create_part_table(create_table_sql, select_sqls, insert_sql)


def prepare_validation_table(pre,select_sqls):
    '''
    构建用于校验规则不一致的隐式转换的join表
    '''
    # 构建用于隐式转换join的表(索引失效）
    create_table_sql = "CREATE TABLE implicit_validation_without_index_table (h_c_id VARCHAR(255), h_data VARCHAR(255), index index_cid(h_data)) default character set utf8 collate utf8_bin"
    insert_sql = "insert into implicit_validation_without_index_table (h_c_id,h_data) values(%s,%s)"
    pre.create_part_table(create_table_sql, select_sqls, insert_sql)

    # 构建用于隐式转换join的表(索引有效）
    create_table_sql = "CREATE TABLE implicit_validation_with_index_table (h_c_id VARCHAR(255), h_data VARCHAR(255), index index_cid(h_data)) default character set utf8 collate utf8_general_ci"
    insert_sql = "insert into implicit_validation_with_index_table (h_c_id,h_data) values(%s,%s)"
    pre.create_part_table(create_table_sql, select_sqls, insert_sql)


select_sqls = ["select h_c_id,h_data from history"]
pre = Preparation()
# prepare_join_table(pre,select_sqls)
prepare_charset_table(pre,select_sqls)
prepare_validation_table(pre,select_sqls)