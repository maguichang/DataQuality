# -*- coding:utf-8 -*-
# @Time 2019/5/7 14:40
# @Author Ma Guichang
"""
status: finished
唯一性：
1.数据内容的唯一性(finished)

"""
import time
import pymysql
import numpy as np
import pandas as pd
from flask import Blueprint,jsonify,request
from dbPool import dbConnect

uniqueness=Blueprint('uniqueness',__name__)

# 从连接池获取数据库连接
db_pool = dbConnect.get_db_pool(False)
conn = db_pool.connection()

@uniqueness.route('/checkUnique/<database>/<table>/<path:field>',methods=['GET','POST'])
def checkUnique(database,table,field):
    """
    唯一性校验
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 校验列在mysql中的存储类型
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    # 读取数据库开始时间
    start_read = time.clock()

    db = database
    tb = table
    fd = field
    sql = "SELECT " + fd + " FROM " + db + "." + tb
    dfData = pd.read_sql(sql, conn)  # 一次性读取，测试性能时可分批读取
    end_read = time.clock()
    print("唯一值校验读取数据库时间: %s Seconds" % (end_read - start_read))

    checkData = dfData.duplicated().value_counts()
    # 重复数据记录数
    if True in checkData.index.values:
        duplicateNum = checkData[True]
    else:
        duplicateNum = 0
    # 唯一数据记录数
    if False in checkData.index.values:
        uniqueNum = checkData[False]
    else:
        uniqueNum = 0
    # 重复数据占比
    duplicateNumRatio = uniqueNum / (int(duplicateNum) + int(uniqueNum))
    res = {"duplicateNum": int(duplicateNum), "uniqueNum": int(uniqueNum), "uniqueNumRatio": duplicateNumRatio}

    end_exec = time.clock()
    print("唯一值校验总用时: %s Seconds" % (end_exec - start_read))
    return jsonify(res)

# 访问示例: 127.0.0.1:5000/uniqueness/checkUnique/datagovernance/fakedata/country
