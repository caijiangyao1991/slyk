#encoding=utf8
__author__ = 'Kevin'
import psycopg2
import pandas as pd
from tool.DataCache.DataCache import DataCache

class DBAccess:


    def __init__(self,area='dy'):
        '''
        初始化        :
        return:
        '''
        self.area = area

    def get_con(self):

        if self.area=='dy':
            return self.__get_dy_con()
        else:
            return self.__get_pzh_con()


    def __get_dy_con(self):
        '''
        获取数据库连接
        :return:
        '''

        # TODO 替换为根据不同的数据库返回不一样的链接，可以通过查看配置表来进行访问
        conn = psycopg2.connect(database="dy",
                                user="gpadmin",
                                password="gpadmin",
                                host="192.168.10.60",
                                port="5432")
        return conn
    def __get_pzh_con(self):
        '''
        获取数据库连接
        :return:
        '''

        # TODO 替换为根据不同的数据库返回不一样的链接，可以通过查看配置表来进行访问
        conn = psycopg2.connect(database="pzh_dw",
                                user="gpadmin",
                                password="gpadmin",
                                host="192.168.10.60",
                                port="5432")
        return conn


    def get_db_df(self,sql):
        '''
        根据sql获取pandas dataframe
        :param sql:
        :return:
        '''

        dc=DataCache()
        if dc.isCached(sql):
            return dc.get_cached_file(sql)
        conn=self.get_con()
        resdf=pd.read_sql(sql,conn)
        dc.save_obj(sql,resdf)

        return resdf

#example
if __name__ == '__main__':
    db=DBAccess('pzh')
    df = db.get_db_df("select count(*) as test from t_jsxx_bc")
    print df.head()