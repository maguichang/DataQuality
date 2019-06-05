# -*- coding:utf-8 -*-
# @Time 2019/5/7 14:38
# @Author Ma Guichang

"""
status: developing
一致性：
1.相同数据的一致性(developing)
    同一数据在不同位置存储或被不同应用或用户使用时数据的一致性，以及不同时间段的一致性即双表count校验

2.关联数据的一致性(finished)
    具有关联关系的字段的数据的一致性（父子表，主外键）.
        2.1 父/子表参考的一致性
        确定父表/子表之间的参考完整性，以找出无父记录的子记录的值
        2.2 子/父表参考的一致性
        确定父表/子表之间的参考完整性，以找出无子记录的父记录的值
3. 一个字段默认值使用的一致性
    评估列属性和数据在可被赋予默认值的每个字段的默认值
4. 跨表的默认值使用的一致性
    评估列属性和数据在相同数据类型的字段默认值上的一致性
"""
import pymysql
import time
import numpy as np
import pandas as pd
from flask import Blueprint,jsonify,request
from dbPool import dbConnect

consistency=Blueprint('consistency',__name__)
# 从连接池获取数据库连接
db_pool = dbConnect.get_db_pool(False)
conn = db_pool.connection()


@consistency.route('/checkDoubleCount/<srcdb>/<srctb>/<srcfd>/<desdb>/<destb>/<desfd>',methods = ['GET','POST'])
def checkDoubleCount(srcdb,srctb,srcfd,desdb,destb,desfd):
    """
    双表count校验
    :param srcdb: 源数据库
    :param srctb: 源数据表
    :param srcfd: 源检验列
    :param desdb: 目标数据库
    :param destb: 目标数据表
    :param desfd: 目标检验例
    :return:
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    sql_tb1 = "SELECT count("+srcfd+") as f1 FROM "+srcdb+"."+srctb
    dfDataTb1 = pd.read_sql(sql_tb1, conn)
    print(dfDataTb1["f1"][0])# numpy.int64数据类型
    sql_tb2 = "SELECT count("+desfd+") as f2 FROM "+desdb+"."+destb
    dfDataTb2 = pd.read_sql(sql_tb2, conn)
    print(dfDataTb2["f2"][0])

    if dfDataTb1["f1"][0] == dfDataTb2["f2"][0]:
        res = 'double count check success'
    else:
        res = 'double count check faild'
    return res


@consistency.route('/checkMasterSlaveTable/<masterDatabase>/<masterTable>/<masterField>/<slaveDatabase>/<slaveTable>/<slaveField>',methods = ['GET','POST'])
def checkMasterSlaveTable(masterDatabase,masterTable,masterField,slaveDatabase,slaveTable,slaveField):
    """
    关联数据的一致性
    :param masterDatabase: 父表数据库
    :param masterTable: 父表数据表
    :param masterField: 父表校验列
    :param slaveDatabase: 子表数据库
    :param slaveTable: 子表数据表
    :param slaveField: 子表校验列
    :return:
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    
    mdb = masterDatabase
    mtb = masterTable
    mfd = masterField
    sdb = slaveDatabase
    stb = slaveTable
    sfd = slaveField

    sql_m = " SELECT " + mfd + " FROM " + mdb + "." + mtb
    mdfData = pd.read_sql(sql_m, conn)

    sql_s = " SELECT " + sfd + " FROM " + sdb + "." + stb
    sdfData = pd.read_sql(sql_s, conn)

    #此处采用循环实现，效率较低，待优化
    # 无父记录的子记录的值
    num = 0
    for i in list(sdfData[sfd]):
        if i not in list(mdfData[mfd]):
            num = num + 1

    # 无子记录的父记录的值
    num2 = 0
    for i in list(mdfData[mfd]):
        if i not in list(sdfData[sfd]):
            num2 = num2 + 1

    return  jsonify({"无父记录的子记录的值":num,"无子记录的父记录的值":num2})

# 访问示例:127.0.0.1:5000/conformity/checkMasterSlaveTable/datagovernance/class/cname/datagovernance/student2/cid

@consistency.route('/checkSingleDefaultValue/<database>/<table>/<field>',methods = ['GET','POST'])
def checkSingleDefaultValue(database,table,field):
    """
    一个字段默认值使用的一致性
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
    sql = "DESC "+db+"."+tb
    dfData = pd.read_sql(sql, conn)

    end_read = time.clock()
    print("一个字段默认值使用的一致性校验读取数据库时间: %s Seconds" % (end_read - start_read))

    defaultDataListDict = dict(zip(list(dfData['Field']),list(dfData['Default'])))
    defaultValue = defaultDataListDict[fd]

    end_exec = time.clock()
    print("默认值一致性校验总用时: %s Seconds" % (end_exec - start_read))
    return  jsonify({fd:defaultValue})

# 访问示例127.0.0.1:5000/conformity/checkSingleDefaultValue/datagovernance/mytable/ff

@consistency.route('/checkDefaultValue/<database>/<table>/<field>',methods = ['GET','POST'])
def checkDefaultValue(database,table,field):
    """
    评估列属性和数据在相同数据类型的字段默认值上的一致性
    :param database: 校验数据库
    :param table: 校验表
    :param field: 校验列
    :return: 返回数据库下所有表的相同数据类型的字段默认值是否一致
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()
    db = database
    tb = table
    fd = field
    sql = "select table_name from information_schema.tables WHERE table_schema = "+"'"+db+"'"
    # 返回指定数据库下所有表名
    dfData = pd.read_sql(sql, conn)
    allTableDefaultValue = {}
    for i in list(dfData['table_name']):
        sql = "DESC "+db+"."+i
        tbData = pd.read_sql(sql, conn)
        # 完全重复的类型与默认值组合会被合并
        print(dict(zip(list(tbData['Type']),tbData['Default'])))
        allTableDefaultValue.setdefault(i, []).append(dict(zip(list(tbData['Type']),tbData['Default'])))
    return jsonify(allTableDefaultValue)

# 访问示例：127.0.0.1:5000/conformity/checkDefaultValue/datagovernance/mytable/ff