import logging
from time import sleep

from Connection.Connection import Database


class Transcation():
    def create_connection(self):
        '''
        创建新连接
        '''
        db = Database()  # 这里必须在新的连接下发起事务，否则会在start_transaction处报错 非DatabaseError
        conn, cur = db.connection2()
        # 事务开始
        conn.begin()
        return conn, cur

    def begin_uncommit_transcations(self, cur, sqls, session_number):
        '''
        发起不提交事务
        '''
        error_sql = "tempt"
        try:
            # 执行事务
            for one_sql in sqls:
                # execution_sql_log
                # logging.basicConfig(filename='D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/LOG/execution_sql.log',
                #                     format='[%(asctime)s-%(filename)s-%(levelname)s:%(message)s]', level=logging.DEBUG,
                #                     filemode='a', datefmt='%Y-%m-%d %I:%M:%S %p')
                # logging.info(str(session_number) + '\t' + str(one_sql))
                error_sql = one_sql
                cur.execute(one_sql)
                # TODO：异常指标检测，此时是锁等待
                sleep(2)
            return str(0), error_sql
        except Exception as e:
            # error_sql_log
            # logging.basicConfig(filename='D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/LOG/execution_sql.log',
            #                     format='[%(asctime)s-%(filename)s-%(levelname)s:%(message)s]', level=logging.DEBUG,
            #                     filemode='a', datefmt='%Y-%m-%d %I:%M:%S %p')
            # logging.error(str(e))
            print(e)
            print(error_sql)
            return e, error_sql

    def begin_uncommit_transcations_debug(self, cur, sqls, blocked_number, session_number):
        '''
        发起不提交事务（用于查找阻塞事务id，执行sql将不记录在log中）
        '''
        error_sql = "tempt"
        try:
            # 执行事务
            for one_sql in sqls:
                error_sql = one_sql
                cur.execute(one_sql)
                # TODO：异常指标检测，此时是锁等待
                sleep(2)
            return str(0), error_sql
        except Exception as e:
            print(e)
            print(error_sql)
            return e, error_sql


    def begin_uncommit_transcation(self, cur, sql, session_number):
        '''
        发起不提交事务
        '''
        try:
            # execution_sql_log
            # logging.basicConfig(filename='/LOG/execution_sql.log',
            #                     format='[%(asctime)s-%(filename)s-%(levelname)s:%(message)s]', level=logging.DEBUG,
            #                     filemode='a', datefmt='%Y-%m-%d %I:%M:%S %p')
            # logging.info(str(session_number) + '\t' + str(sql))
            # 执行事务
            cur.execute(sql)
            sleep(2)
            return str(0)
        except Exception as e:
            # error_sql_log
            # logging.basicConfig(filename='/LOG/execution_sql.log',
            #                     format='[%(asctime)s-%(filename)s-%(levelname)s:%(message)s]', level=logging.DEBUG,
            #                     filemode='a', datefmt='%Y-%m-%d %I:%M:%S %p')
            # logging.error(str(e))
            return e

    def begin_commit_transcation(self, conn, cur, sql):
        '''
        发起提交事务
        '''
        try:
            # 执行事务
            cur.execute(sql)
            conn.commit()
            sleep(2)
            return str(0)
        except Exception as e:
            return e
