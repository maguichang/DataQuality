# -*- coding:utf-8 -*-
# @Time 2019/3/28 11:54
# @Author Ma Guichang

"""
数据库引用的参数的配置的文件
config对象模块－－采用了 基于类继承的config结构，保存默认配置的Config类作为基类，其他类继承之。
"""

class Config(object):
    DEBUG = False
    TESTING = False
    # mongo setup
    # DATABASE_URI = 'ip'
    # DATABASE_NAME = 'kafkaTopic'
    # DATABASE_PORT = 27017
    # DATABASE_USER = 'admin'
    # DATABASE_PASSWORD = 'admin'
    # mysql setup
    MYSQL_URI = '10.0.8.201'
    MYSQL_PORT = 3306
    MYSQL_USER = 'root'
    MYSQL_PWD = 'Mgc5320'
    MYSQL_DB = 'test'
    MYSQL_CHARSET = 'utf8'
    #mail setup
    MAIL_SERVER = 'smtp.163.com'
    MAIL_PORT = 25
    MAIL_USERNAME = '邮箱用户名'
    MAIL_PASSWORD = '邮箱密码'
    MAIL_SENDER = '发送邮箱'
    MAIL_RECIPIENTS = '接收邮箱'



class ProductionConfig(Config):
    DATABASE_URI = '10.0.7.37'
    DATABASE_NAME = 'mgc'


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True