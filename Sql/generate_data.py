import random
import string
import time
from decimal import Decimal
from Connection.Connection import Database


class Columns(object):
    # 初始化过程：
    # 从mysql的数据表：information_schema.COLUMNS 中下载 schema_table 表的相关信息
    # 通过列的数据类型名称 匹配 解析函数，比如 varchar 类型会匹配到 varchar()函数
    # 解析函数会获取到随机函数及随机函数的参数，比如 varchar函数会匹配到随机函数 r_char，及其参数 (10,)
    # 手动指定钩子函数，会强制使某个列使用指定的随机函数及指定的参数
    def __init__(self, database_name, table_name, hook={}):
        # 存放 表列，比如：[u_age,u_score,u_money,update_time,u_birth,u_sex,u_name,u_address]
        self.column_name = []
        # 存放 随机函数（都以“r_”开头），与 column_name 一一对应，比如：
        # [r_int、r_float、r_decimal、r_datetime、r_date、r_enum、r_char]
        self.random_function = []
        # 存放 随机函数的参数（以元组表示），与 random_function 一一对应，比如：
        # [(0,100),(1.0,100.0),(12,2),('1994-02-05 02:03:15','2004-02-05 02:03:15'),
        # ('1994-02-05','2004-02-05'),('Male','Female'),(10,)]
        self.random_function_arg = []
        self.database = database_name
        self.table = table_name
        table_info = "select * from information_schema.COLUMNS where TABLE_SCHEMA='" + self.database + "' and TABLE_NAME='" + self.table + "'"
        db = Database()  # 这里必须在新的连接下发起事务，否则会在start_transaction处报错 非DatabaseError
        conn = db.conn
        cur = db.cursor
        cur.execute(table_info)
        row_all = cur.fetchall()
        for c in row_all:
            cn = c[3]
            # 钩子函数
            if cn in hook:
                self.column_name.append(cn)
                self.random_function.append(hook[cn][0])
                self.random_function_arg.append(hook[cn][1])
                continue
            if c[17] == 'auto_increment':
                continue
            # 通过数据类型的名称自动匹配随机函数
            func, func_arg = getattr(self, c[7])(c)
            self.column_name.append(cn)
            self.random_function.append(func)
            self.random_function_arg.append(func_arg)

    # int型的随机函数，返回一个随机整数
    @staticmethod
    def r_int(down, up):
        return random.randint(down, up)

    @staticmethod
    def r_float(down, up):
        return random.uniform(down, up)

    @staticmethod
    def r_decimal(m=5, n=1):
        int_str = "{}.{}".format(''.join(random.sample(string.digits, m - n)),
                                 ''.join(random.sample(string.digits, n)))
        return str(Decimal(int_str))

    @staticmethod
    def r_datetime(begin='1970-01-01 08:00:00', end='2038-01-19 11:14:07'):
        f = "%Y-%m-%d %H:%M:%S"
        begin = time.mktime(time.strptime(begin, f))
        end = time.mktime(time.strptime(end, f)) if end != 'now' else time.time()
        ts = random.randint(int(begin), int(end))
        t = time.localtime(ts)
        return time.strftime("%Y-%m-%d %H:%M:%S", t)

    @staticmethod
    def r_date(begin='1970-01-02', end='2038-01-19'):
        f = "%Y-%m-%d"
        begin = time.mktime(time.strptime(begin, f))
        end = time.mktime(time.strptime(end, f)) if end != 'now' else time.time()
        ts = random.randint(int(begin), int(end))
        t = time.localtime(ts)
        return time.strftime("%Y-%m-%d", t)

    @staticmethod
    def r_enum(*choices):
        return random.choice(choices)

    @staticmethod
    def r_char(m):
        size = random.randint(1, m)
        return ''.join(random.sample(string.ascii_letters, size))

    @staticmethod
    def r_text(m):
        size = random.randint(1, m)
        return ''.join(random.sample(string.ascii_letters, size))

    # 解析函数：会返回一个随机函数，并从 information_schema.COLUMNS 的数据中确定随机函数的参数（一般就是取值范围之类的）
    # 比如 mysql user 表的 u_age int unsigned ，会返回 'r_int',(0,255)
    # 如果想让 age的取值范围为(1,100),则需从钩子函数中指定
    @staticmethod
    def tinyint(c):
        unsigned = c[15].lstrip('tinyint ')
        (down, up) = (0, 25) if unsigned else (-18, 17)
        return 'r_int', (down, up)

    @staticmethod
    def smallint(c):
        unsigned = c[15].lstrip('smallint ')
        # TODO
        (down, up) = (0, 25) if unsigned else (-18, 17)
        return 'r_int', (down, up)

    @staticmethod
    def medimuint(c):
        unsigned = c[15].lstrip('medimuint ')
        (down, up) = (0, 25) if unsigned else (-18, 17)
        return 'r_int', (down, up)

    @staticmethod
    def int(c):
        unsigned = c[15].lstrip('int ')
        (down, up) = (0, 25) if unsigned else (-18, 17)
        return 'r_int', (down, up)

    @staticmethod
    def bigint(c):
        unsigned = c[15].lstrip('bigint ')
        (down, up) = (0, 25) if unsigned else (-18, 17)
        return 'r_int', (down, up)

    @staticmethod
    def float(c):
        unsigned = c[15].lstrip('float ')
        (down, up) = (0, 1E+3) if unsigned else (-1E+3, 1E+3)
        return 'r_float', (down, up)

    @staticmethod
    def double(c):
        unsigned = c[15].lstrip('double ')
        (down, up) = (0, 1E+3) if unsigned else (-1E+3, 1E+3)
        return 'r_float', (down, up)

    @staticmethod
    def decimal(c):
        m = c[10]
        n = c[11]
        return 'r_decimal', (m, n)

    @staticmethod
    def date(c):
        return 'r_date', ()

    @staticmethod
    def datetime(c):
        return 'r_datetime', ()

    @staticmethod
    def enum(c):
        vals = eval(c[15].lstrip('enum'))
        return 'r_enum', tuple(vals)

    @staticmethod
    def varchar(c):
        m = c[8]
        if m > 26:
            m = 26
        return 'r_char', (m,)

    @staticmethod
    def char(c):
        m = c[8]
        if m > 26:
            m = 26
        return 'r_char', (m,)

    @staticmethod
    def text(c):
        m = c[8]
        if m > 26:
            m = 26
        return 'r_text', (m,)

    # 返回mysql表的随机数据
    def call_row(self,sql_number):
        datas = []
        for j in range(sql_number):
            one_data = []
            for i in range(len(self.random_function)):
                data = getattr(self, self.random_function[i])(*self.random_function_arg[i])
                one_data.append(data)
            datas.append(one_data)
        return datas

