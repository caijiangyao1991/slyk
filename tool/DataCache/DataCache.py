##encoding=utf8
__author__ = 'Administrator'

import cPickle
import os
import hashlib
import logging
import pandas as pd

class DataCache:

    ##todo 路径修改
    initPath=u"./sqlcache/default.cache"
    cachMap={}
    isFirst=False

    def __init__(self):
        """
        缓存取初始化
        :return:
        """
        if not os.path.exists('./sqlcache/'):
            os.mkdir('./sqlcache/')

        isExist = os.path.exists(self.initPath)
        if isExist:
            f=open(self.initPath)
            self.cachMap=cPickle.load(f)
            self.isFirst=True
            f.close()

    def isCached(self,name):
        return name in self.cachMap.keys()

    def get_cached_file(self,name):
        filepath=self.cachMap.get(name)
        f=open("./sqlcache/"+filepath)
        res=cPickle.load(f)
        return res

    def save_obj(self,name,saveObj,isUpdated=False):
        '''
        将当前的数据持久化数据保存
        :param name:字符串名称（sql查询等）
        :param saveObj:结果数据（sql数据查询出来的结果）
        :return:
        '''

        if name in self.cachMap.keys() and not isUpdated:
           print ("the "+ name+" is saveed ")
           return

        m=hashlib.md5()
        m.update(name)
        mapStr=m.hexdigest()
        f=open("./sqlcache/"+mapStr,'w')
        cPickle.dump(saveObj,f)

        self.cachMap[name]=mapStr
        f=open(self.initPath,'w')
        cPickle.dump(self.cachMap,f)


if __name__ == '__main__':
    dc=DataCache()
    stra="test2"
    amap={"3":"3","34":"32"}

    xx= dc.get_cached_file(stra)

    print type(xx)

    # df=pd.DataFrame([[1,2],[3,4]])
    #
    # dc.save_obj(stra,df,isUpdated=True)
