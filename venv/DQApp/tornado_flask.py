# -*- coding:utf-8 -*-
# @Time 2019/5/16 13:16
# @Author Ma Guichang

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from main import app
http_server = HTTPServer(WSGIContainer(app))
http_server.listen(5000)
IOLoop.instance().start()