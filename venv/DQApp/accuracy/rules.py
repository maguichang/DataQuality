# -*- coding:utf-8 -*-
# @Time 2019/4/30 16:29
# @Author Ma Guichang
"""
status: developing
准确性：
1.数据内容的正确性(developing)
    数值范围（值域校验）
    精度校验
2.数据格式的合规性(finished)
    数据记录只要存在信息即表征记录完整，记录中的缺失信息不表示记录不完整.
        a) 类型
        b) 长度
        c) 精度
3.数据重复率(finished)
    校验列
4.脏数据出现率(finished)
    a) 离群值检查用于检查数据中是否有一个或几个数值与其他数值相比差异较大。
    b) 通过计算出指标的算术平均值和标准差后，根据拉依达法或者格鲁布斯法检查数据中与其他数值相比差异较大的数。
    ?) 临界值表中的T的获取（待处理）
"""
import re
import time
import pymysql
import numpy as np
from datetime import datetime
import pandas as pd
from validators import between,length,email
from flask import Blueprint,jsonify,request
from DQApp.dbPool import dbConnect

accuracy=Blueprint('accuracy',__name__)


@accuracy.route('/checkType/<database>/<table>/<path:field>',methods=['GET','POST'])
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

# 访问示例: 127.0.0.1:5000/accuracy/checkType/datagovernance/testdata/data,address,name


@accuracy.route('/checkValueRange/<rangeType>/<minValue>/<maxValue>/<database>/<table>/<field>',methods=['GET','POST'])
def checkValueRange(rangeType,minValue,maxValue,database,table,field):
    """
    值域校验
    :param rangeType: 校验的值域类型，数值型（取值范围）or字符型（枚举区间）or 时间范围
    :param minValue: 下限
    :param maxValue: 上限
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

    sqlCount = "SELECT COUNT(*) as recordCount FROM " + db + "." + tb
    CountNum = pd.read_sql(sqlCount,conn)

    end_read = time.clock()
    print("列类型校验读取数据库时间: %s Seconds" % (end_read - start_read))


    recordCount = CountNum.get('recordCount').values[0]
    accordValueRangeNum = 0
    sql = "SELECT " + fd + " from " + db + "." + tb
    dfData = pd.read_sql(sql, conn, chunksize=20000)
    # print(minValue,maxValue)

    # 检验该列数据的值域
    if rangeType == 'valueRange':
        for df in dfData:
            for d in df[fd]:
                # if between(d, min=np.float(minValue), max=np.float(maxValue)):# 校验的效率比较低
                if np.float(minValue)<d<np.float(maxValue):# 校验的效率比较低
                    accordValueRangeNum = accordValueRangeNum + 1
        # return jsonify({"accordValueRangeRatio": accordValueRangeNum / recordCount})
    # 字符枚举
    elif rangeType == 'charRange':
        for df in dfData:
            for d in df[fd]:
                if d in [minValue, maxValue]:
                    accordValueRangeNum = accordValueRangeNum + 1
        # return jsonify({"accordValueRangeRatio": accordValueRangeNum/recordCount})
    # 时间区间
    elif rangeType == 'dateRange':
        mindate = datetime.strptime(minValue, '%Y-%m-%d %H:%M:%S')
        maxdate = datetime.strptime(maxValue, '%Y-%m-%d %H:%M:%S')
        for df in dfData:
            for d in df[fd]:
                if mindate < d < maxdate:
                    accordValueRangeNum = accordValueRangeNum + 1
        # return jsonify({"accordValueRangeRatio": accordValueRangeNum/recordCount})

    end_exec = time.clock()
    print("列类型校验总用时: %s Seconds" % (end_exec - start_read))
    return jsonify({"accordValueRangeRatio": accordValueRangeNum / recordCount})

# 访问示例：
# 127.0.0.1:5000/accuracy/checkValueRange/valueRange/50/80/datagovernance/fakedata/age

@accuracy.route('/checkPrecision/<database>/<table>/<field>',methods=['GET','POST'])
def checkPrecision(database,table,field):
    """
    精度校验，校验小数点位数(计量单位校验待定)
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 校验列的小数点位数
    """
    # 从连接池获取数据库连接
    db_pool = dbConnect.get_db_pool(False)
    conn = db_pool.connection()

    db = database
    tb = table
    fd = field
    sql = "SHOW FULL COLUMNS FROM "+db+"."+tb
    columnInfo = pd.read_sql(sql, conn)

    # 获取校验列的精度信息，计量单位，小数位数
    precisionInfo = columnInfo[['Field','Type','Comment']]
    # data为校验字段
    xsws = precisionInfo['Type'][precisionInfo['Field']== fd]
    typeSplit = re.split(r"[',',')']", xsws.values[0])
    if len(typeSplit) > 2:
        # decimalDigits为小数位数
        _, decimalDigits, _ = typeSplit

        end_exec = time.clock()
        print("精度校验总用时: %s Seconds" % (end_exec - start_read))
        return jsonify({fd:decimalDigits})
    else:
        return "校验列没有小数位"

# 访问示例：127.0.0.1:5000/accuracy/checkPrecision/datagovernance/testdata/data


@accuracy.route('/checkRepeat/<database>/<table>/<field>',methods=['GET','POST'])
def checkRepeat(database,table,field):
    """
    数据重复率
    :param database: 校验数据库
    :param table: 校验数据表
    :param field: 校验列
    :return: 返回校验列数据重复率
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
    print("重复率校验读取数据库时间: %s Seconds" % (end_read - start_read))

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
    duplicateNumRatio = duplicateNum / (int(duplicateNum) + int(uniqueNum))
    res = {"duplicateNum": int(duplicateNum), "uniqueNum": int(uniqueNum), "duplicateNumRatio": duplicateNumRatio}

    end_exec = time.clock()
    print("重复率校验总用时: %s Seconds" % (end_exec - start_read))
    return jsonify(res)

# 访问示例：127.0.0.1:5000/accuracy/checkRepeat/datagovernance/testdata/address


@accuracy.route('/checkOutlier/<funType>/<database>/<table>/<field>',methods = ['GET','POST'])
def checkOutlier(funType,database,table,field):
    """
    离群值检查
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
    sql = "SELECT "+fd+ " FROM "+db+"."+tb
    dfData = pd.read_sql(sql, conn)
    end_read = time.clock()
    print("离群值校验读取数据库时间: %s Seconds" % (end_read - start_read))

    dataMean = dfData.mean()[fd]
    dataStd = dfData.std()[fd]
    if funType == "3S":
        # 3S法则计算：当某一测量数据与其测量结果的算术平均值之差大于3倍标准偏差时，则该检查数据不符合规则。
        res = np.abs(np.array(dfData.get(fd))-dataMean)-3*dataStd
        # 尝试使用numpy优化
        outlierNum = len([d for d in res if d > 0])

        end_exec = time.clock()
        print("离群值校验总用时: %s Seconds" % (end_exec - start_read))
        return jsonify({"outlierNum": str(outlierNum)})
    # elif funType == "glbs":
    #     # 格鲁布斯法：计算公式为T = | X质疑—X平均 | / S，其中，S为这组数据的标准差(?需要对比临界值表中的T(待定))
    #     Tdata = np.abs(np.array(dfData.get(fd)) - dataMean)/dataStd
    #     # 表达式中的T为临界值表中的T
    #     outlierNum = len([d for d in Tdata if d-T > 0])
    #     pass