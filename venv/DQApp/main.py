# -*- coding:utf-8 -*-
# @Time 2019/4/1 13:38
# @Author Ma Guichang

from flask import Flask
from completeness.rules import *
from DQApp.accuracy.rules import *
from DQApp.uniqueness.rules import *
from DQApp.conformity.rules import *
from DQApp.timeliness.rules import *
from DQApp.normalization.rules import *
# 多进程模块
from multiprocessing import Pool
# 多线程模块
import threading
from gevent.pywsgi import WSGIServer
from gevent import monkey
monkey.patch_all()

app = Flask(__name__)
app.register_blueprint(completeness,url_prefix='/completeness')
app.register_blueprint(accuracy,url_prefix='/accuracy')
app.register_blueprint(uniqueness,url_prefix='/uniqueness')
app.register_blueprint(conformity,url_prefix='/conformity')
app.register_blueprint(timeliness,url_prefix='/timeliness')
app.register_blueprint(normalization,url_prefix='/normalization')


if __name__=='__main__':

  WSGIServer(('0.0.0.0',5000),app).serve_forever()
  # app.run(threaded = True)
  # developing -> master
