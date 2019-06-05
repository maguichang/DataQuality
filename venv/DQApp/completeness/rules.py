# -*- coding:utf-8 -*-
# @Time 2019/4/29 16:58
# @Author Ma Guichang
"""
status: finished
完整性：
1.数据元素的完整性
    空值校验，检验列（单列或多列实现（多列传入为可变个数参数））
        a) 空值定义，在mysql中除String类型数据存的数据为空字符串值，其余数据类型的空值为null值
        b) 校验结果返回空值数据所占百分比
2.数据记录的完整性
    数据记录只要存在信息即表征记录完整，记录中的缺失信息不表示记录不完整.
        a) 与输入值的比较
        b) 与表count比较
3.元数据与参考数据的充分性
    判断给定的字段是否在参考的数据表中
"""
import pymysql
import requests
import numpy as np
import pandas as pd
import time
import datetime
from flask import Blueprint,jsonify,request
from dbPool import dbConnect

completeness=Blueprint('completeness',__name__)

@completeness.route('/elementCheckNull/<database>/<table>/<path:field>',methods=['GET','POST'])
def elementCheckNull(database,table,field):
    """
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 非空值元素所占百分比
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    # 读取数据库开始时间
    start_read = time.clock()

    db = database
    tb = table
    fd = field

    all_num = 0
    null_list = []
    sql = "SELECT "+fd+" FROM "+db+"."+tb
    dfData = pd.read_sql(sql, conn,chunksize=20000)

    # 读取数据库开始时间
    end_read = time.clock()
    print("空值校验读取数据库时间: %s Seconds"%(end_read-start_read))


    for df in dfData:
        # 返回dataframe的行列数，shape[0]行数或者len(df)，shape[1]列数
        all_num = all_num + len(df)
        null_list.append(df.isna().sum().values)
    # 按列相加
    null_array = np.array(null_list).sum(axis=0)
    # 如若设定保留小数，可能极小值显示为0，影响显示精度
    # nullDataRatio = np.around(null_array / all_num, 3)
    nullDataRatio = 1 - null_array / all_num
    res = dict(zip(fd.split(','),nullDataRatio))

    end_exec = time.clock()
    print("空值校验总用时: %s Seconds" % (end_exec - start_read))

    return  jsonify(res)

# 访问示例 127.0.0.1:5000/completeness/elementCheckNull/datagovernance/fakedata/age
# 访问示例 127.0.0.1:5000/completeness/elementCheckNull/datagovernance/testdata/address,data


@completeness.route('/recordCheckNull/<chooseFlag>/<database>/<table>/<path:inputValue>',methods=['GET','POST'])
def recordCheckNull(chooseFlag,database,table,inputValue):
    """
    记录的完整性校验
    :param chooseFlag: 选择的校验方式，与输入值比较OR表count值比较,ic(input constant),tc(table count)
    :param database: 校验的数据库，根据chooseFlag的选择，判定是同源还是跨源
    :param table: 校验的数据表，根据chooseFlag的选择，判定是同表还是跨表
    :return: 符合完整性记录的百分比
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()

    # 读取数据库开始时间
    start_read = time.clock()
    # 从连接池获取数据库连接
    print("获取数据库连接")
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()

    db = database
    tb = table
    inputValue = inputValue

    sql = "SELECT COUNT(*) as recordCount FROM " + db + "." + tb
    dfData = pd.read_sql(sql, conn)

    # 读取数据库结束时间
    end_read = time.clock()
    print("记录完整性校验读取数据库时间: %s Seconds" % (end_read - start_read))

    recordCount = dfData.get('recordCount').values

    if chooseFlag == 'ic':
        res = recordCount[0]/np.float(inputValue)

    elif chooseFlag == 'tc':
        inputValue = inputValue.replace(',','.')
        sql_dest = "SELECT COUNT(*) as destCount FROM " + inputValue
        dfData_dest = pd.read_sql(sql_dest, conn)
        recordCount_dest = dfData_dest.get('destCount').values
        res = recordCount[0]/recordCount_dest[0]

    end_exec = time.clock()
    print("记录完整性校验总用时: %s Seconds" % (end_exec - start_read))
    return jsonify({"recordNotnullRatio":res})


# 访问示例：
# 127.0.0.1:5000/completeness/recordCheckNull/ic/datagovernance/fakedata/4000000
# 127.0.0.1:5000/completeness/recordCheckNull/tc/datagovernance/testdata/datagovernance,t1

@completeness.route('/checkFieldInTable/<database>/<table>/<checkField>',methods = ['GET','POST'])
def checkMetadataAndComparabeData(database,table,checkField):
    """
    元数据与参考数据的充分性
    :param database: 校验数据库
    :param table: 校验数据表
    :param checkField: 校验列
    :return: 检验字段是否存在参考数据表中
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()

    db = database
    tb = table
    checkField = checkField

    sql = "DESC "+db+"."+tb
    dfData = pd.read_sql(sql, conn)

    metaField = list(dfData['Field'])
    if checkField in metaField:
        return "chenckField in metaData,and checkeField is "+checkField
    else:
        return "chenckField not in metaData"
# 访问示例：127.0.0.1:5000/completeness/checkMetadataAndComparabeData/datagovernance/testdata/name