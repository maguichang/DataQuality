# -*- coding:utf-8 -*-
# @Time 2019/5/10 15:39
# @Author Ma Guichang
"""
规范性
1.数据格式的合规性(finished)
    数据记录只要存在信息即表征记录完整，记录中的缺失信息不表示记录不完整.
        a) 类型
        b) 长度（string类型的数据长度，与期望值进行比较）
2.单字段主键校验
    检验列，判断该字段的是否为主键
3.联合主键校验
    检验列，判断组合字段是否为联合主键
4.格式校验
     a) 输入列名，检验该列的数据的格式是否符合要求（以邮箱、手机号、身份证格式为例）
     b) 数据格式:邮箱，身份证，手机号
"""

import pymysql
import time
import numpy as np
import pandas as pd
# 校验值域，长度，邮件
from validators import between,length,email
from flask import Blueprint,jsonify,request
from DQApp.dbPool import dbConnect
from DQApp.normalization.phoneAndCardsCheck import *

normalization=Blueprint('normalization',__name__)



@normalization.route('/checkType/<database>/<table>/<path:field>',methods=['GET','POST'])
def checkType(database,table,field):
    """
    列类型校验
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 校验列在mysql中的存储类型
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    db = database
    tb = table
    fd = field
    sql = "DESC "+db+"."+tb
    dfData = pd.read_sql(sql, conn)
    columnType = dfData[['Field','Type']]
    columnType.set_index(['Field'], inplace=True)
    # 获取单个或多个字段的数据类型
    res = columnType.loc[fd.split(',')]
    res = res.to_dict()
    # 返回数据类型，长度，精度
    return jsonify(res)

# 访问示例: 127.0.0.1:5000/normalization/checkType/datagovernance/testdata/data,address,name


@normalization.route('/checkLength/<db>/<tb>/<fd>/<inputLength>/<compType>',methods=['GET','POST'])
def checkLength(db,tb,fd,inputLength,compType):
    """
    校验数据长度
    :param db: 校验数据库
    :param tb: 校验数据表
    :param fd: 校验列
    :param inputLength: 输入的期望长度
    :param compType: 比较类型（>,<,=,<=,>=）
    :return:
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    sql = "SELECT "+fd+" FROM "+db+"."+tb
    columnInfo = pd.read_sql(sql, conn)
    # 返回每一条记录的字符串的长度，效率比for循环更高,可以加条件过滤、统计长度不符合要求的字符串数据
    strInfo = columnInfo[fd].str
    lengthInfo = columnInfo[fd].str.len()
    if compType == "morethan":
        resRatio = lengthInfo[lengthInfo>int(inputLength)].shape[0]/lengthInfo.shape[0]
    elif compType == "lessthan":
        resRatio = lengthInfo[lengthInfo < int(inputLength)].shape[0] / lengthInfo.shape[0]
    elif compType == "equal":
        resRatio = lengthInfo[lengthInfo == int(inputLength)].shape[0] / lengthInfo.shape[0]

    return jsonify({"maxlengeth":max(lengthInfo),"minlengeth":min(lengthInfo),"accordRatio":resRatio})

# 访问示例：127.0.0.1:5000/normalization/checkLength/datagovernance/testdata/address/5/equal


@normalization.route('/checkPri/<database>/<table>/<field>',methods=['GET','POST'])
def checkPri(database,table,field):
    """
    单字段主键校验
    :param database: 校验数据库名称
    :param table: 校验数据表
    :param field: 校验列
    :return: 返回主键校验结果
    """
    # 读取数据库开始时间
    start_read = time.clock()

    # 从连接池获取数据库连接
    print("获取数据库连接")
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()

    db = database
    tb = table
    fd = field
    sql = "DESC "+db+"."+tb
    # print(sql)
    columnInfo = pd.read_sql(sql, conn)

    end_read = time.clock()
    print("单字段主键校验读取数据库时间: %s Seconds" % (end_read - start_read))

    # 获取某个字段的数据主键信息
    isPri, = columnInfo['Key'][columnInfo['Field'] == fd].values
    # print(isPri)
    if isPri == 'PRI':

        end_exec = time.clock()
        print("单字段主键校验总用时: %s Seconds" % (end_exec - start_read))

        return jsonify({fd:isPri})
    else:
        return jsonify({fd:"NOT PRI"})


@normalization.route('/checkUnionPri/<database>/<table>/<field>',methods=['GET','POST'])
def checkUnionPri(database,table,field):
    """
    联合主键校验
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 返回联合主键校验结果
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    db = database
    tb = table
    fd = field
    sql = "DESC "+db+"."+tb
    columnInfo = pd.read_sql(sql, conn)
    # 获取检验列联合主键信息
    uPriInfo = columnInfo[['Field','Type']][columnInfo['Key']=='PRI']
    uPriList = list(uPriInfo['Field'])
    # 是否需要返回联合主键列表
    if fd in uPriList and len(uPriList)>1:
        return jsonify({fd:"unionPri"})
    else:
        return jsonify({fd:"NOT unionPri"})

@normalization.route('/checkFormat/<typeFormat>/<database>/<table>/<field>',methods = ['GET','POST'])
def checkFormat(typeFormat,database,table,field):
    """
    邮箱、手机号码、身份证格式校验
    :param typeFormat: 校验类型
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    # 读取数据库开始时间
    start_read = time.clock()


    db = database
    tb = table
    fd = field
    sql = "SELECT "+fd+" from "+db+"."+tb
    dfData = pd.read_sql(sql, conn, chunksize=20000)

    end_read = time.clock()
    print("邮箱校验读取数据库时间: %s Seconds" % (end_read - start_read))


    email_num = phone_num = card_num = 0
    # 检验该列所有数据的格式
    if typeFormat == 'email':
        for df in dfData:
            for data in df[fd]:
                if email(data):
                    email_num = email_num+1
        end_exec = time.clock()
        print("邮箱校验总用时: %s Seconds" % (end_exec - start_read))
        return jsonify({fd + " format correct num":int(email_num)})
    elif typeFormat == 'phone':
        for df in dfData:
            for data in df[fd]:
                if checkPhone(str(data)):
                    phone_num = phone_num + 1

        end_exec = time.clock()
        print("电话校验总用时: %s Seconds" % (end_exec - start_read))
        return jsonify({fd + " format correct num": int(phone_num)})
    elif typeFormat == 'idCard':
        for df in dfData:
            for data in df[fd]:
                # checkRes = checkIdcard(data)
                # if checkRes != '':
                #     print(checkRes)
                if data is not None:
                    if checkIdcard(str(data)) == True:
                        card_num = card_num + 1
        end_exec = time.clock()
        print("身份证校验总用时: %s Seconds" % (end_exec - start_read))
        return jsonify({fd + "format correct num": int(card_num)})
# 访问示例:127.0.0.1:5000/normalization/checkFormat/idCard/datagovernance/testdata/idcard
# 127.0.0.1:5000/normalization/checkFormat/email/datagovernance/fakedata/emailinfo