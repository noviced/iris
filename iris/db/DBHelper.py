# coding:utf-8

from  abc import ABCMeta,abstractmethod

class _DBHelper(metaclass=ABCMeta):

    def __init__(self, uri='', username='', password=''):
        pass

    @abstractmethod
    def get_conn(self):
        pass
    @abstractmethod
    def get_conn(self):
        pass

    @abstractmethod
    def get_all(self, sql,param=None):
        pass


    @abstractmethod
    def get_one(self, sql,param=None):
        pass

    @abstractmethod
    def update(self, sql,param=None):
        pass

    @abstractmethod
    def get_df(self, sql):
        pass

    @abstractmethod
    def insert_df(self, df, param=None):
        pass

    @abstractmethod
    def dispose(self):
        pass



