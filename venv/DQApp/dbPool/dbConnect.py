# -*- coding:utf-8 -*-
# @Time 2019/5/14 13:43
# @Author Ma Guichang

"""
数据库连接工具类

"""
import pymysql
from Configlist import dbConfig
from DBUtils.PooledDB import PooledDB, SharedDBConnection
from DBUtils.PersistentDB import PersistentDB, PersistentDBError, NotSupportedError

# 导入数据库配置参数
para = dbConfig.Config()

config = {
    'host': para.MYSQL_URI,
    'port': para.MYSQL_PORT,
    'database': para.MYSQL_DB,
    'user': para.MYSQL_USER,
    'password': para.MYSQL_PWD,
    'charset': para.MYSQL_CHARSET
}

def get_db_pool(is_mult_thread):
    if is_mult_thread:
        poolDB = PooledDB(
            # 指定数据库连接驱动
            creator=pymysql,
            # 连接池允许的最大连接数,0和None表示没有限制
            maxconnections=10,
            # 初始化时,连接池至少创建的空闲连接,0表示不创建
            mincached=2,
            # 连接池中空闲的最多连接数,0和None表示没有限制
            maxcached=5,
            # 连接池中最多共享的连接数量,0和None表示全部共享(其实没什么卵用)
            maxshared=3,
            # 连接池中如果没有可用共享连接后,是否阻塞等待,True表示等等,
            # False表示不等待然后报错
            blocking=True,
            # 开始会话前执行的命令列表
            setsession=[],
            # ping Mysql服务器检查服务是否可用
            ping=0,
            **config
        )
    else:
        poolDB = PersistentDB(
            # 指定数据库连接驱动
            creator=pymysql,
            # 一个连接最大复用次数,0或者None表示没有限制,默认为0
            maxusage=1000,
            **config
        )
    return poolDB


# if __name__ == '__main__':
#     # 以单线程的方式初始化数据库连接池
#     db_pool = get_db_pool(False)
#     # 从数据库连接池中取出一条连接
#     conn = db_pool.connection()
#     cursor = conn.cursor()
#     # 随便查一下吧
#     cursor.execute('select * from aaa')
#     # 随便取一条查询结果
#     result = cursor.fetchone()
#     print(result)
#     # 把连接返还给连接池
#     conn.close()
