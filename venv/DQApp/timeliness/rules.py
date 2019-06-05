# -*- coding:utf-8 -*-
# @Time 2019/5/7 15:53
# @Author Ma Guichang
"""
status: finished
及时性：
1.基于时间点的及时性(finished)
    a)基于时间戳的记录数、频率分布或延迟时间符合业务需求的程度
    b)原始记录产生的时间(业务时间)，处理完数据的时间（处理时间），做差和阈值比较，阈值需给定或者与历史值比价（环比，持续测量）
    c)计算出的时间差值与输入值进行比较，返回符合的记录占比
"""
import pymysql
import time
import numpy as np
import pandas as pd
from flask import Blueprint,jsonify
from dbPool import dbConnect

timeliness=Blueprint('timeliness',__name__)
# 从连接池获取数据库连接
db_pool = dbConnect.get_db_pool(False)
conn = db_pool.connection()


@timeliness.route('/checkTimeliness/<database>/<table>/<interval>/<inputvalue>/<comptype>',methods = ['GET','POST'])
def checkTimeliness(database,table,interval,inputvalue,comptype):
    """
    及时性
    :param database: 校验数据库
    :param table: 校验数据表
    :param interval: 时间差，间隔计量单位，
    SECOND 秒 SECONDS
    MINUTE 分钟 MINUTES
    HOUR 时间 HOURS
    DAY 天 DAYS
    MONTH 月 MONTHS
    YEAR 年 YEARS
    :param inputvalue: 输入参考值
    :param comptype: 比较方式，大于，大于等于，等于，小于，小于等于
    :return: 符合输入数据条件的记录数占比
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    # 读取数据库开始时间
    start_read = time.clock()


    db = database
    tb = table
    interval = interval
    inputvalue = int(inputvalue)
    comptype = comptype
    print(inputvalue,comptype)

    # 生产环境需修改sql的条件语句
    sql = "SELECT TIMESTAMPDIFF("+interval+",starttime,endtime) as difftime FROM "+db+"."+tb+" WHERE starttime IS NOT NULL"
    dfData = pd.read_sql(sql, conn)
    end_read = time.clock()
    print("及时性校验读取数据库时间: %s Seconds" % (end_read - start_read))


    if comptype == "morethan":
        res = dfData.loc[(dfData['difftime'] > inputvalue)].shape[0]/dfData.shape[0]
    elif comptype == "moreOrEqual":
        res = dfData.loc[(dfData['difftime'] >= inputvalue)].shape[0] / dfData.shape[0]
    elif comptype == "Equal":
        res = dfData.loc[(dfData['difftime'] == inputvalue)].shape[0] / dfData.shape[0]
    elif comptype == "lessthan":
        res = dfData.loc[(dfData['difftime'] < inputvalue)].shape[0] / dfData.shape[0]
    elif comptype == "lessOrEqual":
        res = dfData.loc[(dfData['difftime'] <= inputvalue)].shape[0] / dfData.shape[0]

    end_exec = time.clock()
    print("及时性校验总用时: %s Seconds" % (end_exec - start_read))
    return jsonify({"accordTimelinessRatio":res})

# 访问示例: 127.0.0.1:5000/timeliness/checkTimeliness/datagovernance/testdata/HOUR/3/morethan