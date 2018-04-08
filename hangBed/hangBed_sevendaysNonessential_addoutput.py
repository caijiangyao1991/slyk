# coding: utf-8
import pandas as pd
import numpy as np
import sys
import logging
from configparser import ConfigParser
from openpyxl import load_workbook
import os
import shortuuid
from sqlalchemy import create_engine
reload(sys)
sys.setdefaultencoding('gbk')

pd.set_option('display.max_columns',None)

class logger():
    def __init__(self,path):
        self.path = path
        self.logger = self.__ybxx(self.path)

    def __ybxx(self,path):
        path1 = path + u'\log.log'
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            filename='%s' % path1,
                            filemode='w+')
        #create logger
        logger_name =  "hangBed_sevendaysNonessential"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        # create file handler
        fh = logging.StreamHandler()
        fh.setLevel(logging.INFO)

        # create formatter
        fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d  %(message)s"
        datefmt = "%a %d %b %Y %H:%M:%S"
        formatter = logging.Formatter(fmt, datefmt)
        # add handler and formatter to logger
        fh.setFormatter(formatter)
        logging.getLogger('').addHandler(fh)
        return logger


def readData(data):
    data.rename(columns={u'分类名称':'category',u'医院编码':'hosid',u'就诊登记号':'clinid',u'药品剂型':'drugForm'},inplace=True)
    #去掉时间发生错误的比如2099/12/13这种
    data['year'] = data[u'明细发生时间'].str.extract("(\d{4})").astype(int)
    data = data[(data['year']<2020)&(data['year']>2000)]
    data['feetime'] = data[u'明细发生时间'].apply(lambda x: pd.to_datetime(x).date())
    data['intime'] = data[u'入院时间'].apply(lambda x: pd.to_datetime(x).date())
    data['outtime'] = data[u'出院时间'].apply(lambda x: pd.to_datetime(x).date())
    #根据医院、就诊id、入院时间排序
    data_1 = data.sort_values(by=['hosid','clinid','intime'])
    return data_1

#无实质性治疗，只有非注射用药、床位、护理、其他诊疗
def flag(lists):
    if lists.isin([ u'床位',u'护理',u'其它诊疗']).all():
        return 1
    else:
        return 0

def findMaxConsecutiveOnes(numsTime):
    nums = list(numsTime['flag'])
    time = list(numsTime['feetime'])
    cnt = 0
    ans = 0
    endtime ={}
    startTime =''
    for i,j in zip(nums,time):
        if i ==1:
            cntime = j
            cnt = cnt +1
            ans = max(ans, cnt)
            if ans not in endtime.keys():
                endtime[ans] = cntime
        else:
            cnt = 0
    # print "endtime:" + str(endtime)
    # print "ans:" + str(ans)
    endTime = endtime.get(ans)
    for index,num in enumerate(time):
        if num==endTime:
            st_index = index-ans+1
            startTime = time[st_index]
    startTime = pd.to_datetime(startTime).date()
    # print "startTime:" + str(startTime)
    return ans, startTime, endTime

def mkdir(path,log):
    # 引入模块
    isExists = os.path.exists(path)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        log.info('%s directory create success' % path)
        #print path + ' 创建成功'
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        log.info('%s directory already exists' % path)
        #print path + ' 目录已存在'
        return False


if __name__=='__main__':
    #TODO 获取当前目录
    path = os.path.split(os.path.realpath(__file__))[0]
    path.decode('gbk')
    logger1 = logger(path)
    log = logger1.logger
    # TODO 开始运行
    log.info('run begin')

    # TODO 创建结果目录
    resultPath = path + '/result'
    mkdir(resultPath,log)

    # TODO 读取配置文件
    log.info('Read configuration file begin')
    cf = ConfigParser()
    Locpath = path + u'\\Config.ini'
    cf.read(Locpath)
    days = cf.getint('parameter', 'days')
    username = cf.get('database', 'username')
    password = cf.get('database', 'password')
    tns = cf.get('database', 'tns')
    databasename = cf.get('database', 'databasename')
    table = cf.get('database', 'table')
    tabledetail = cf.get('database', 'tabledetail')
    log.info('Read configuration file end')

    # TODO 读取数据
    log.info('Read data begin')
    datapath = path + u'\\data'
    listDir = os.listdir(datapath)
    for i in range(len(listDir)):
        filePath = os.path.join(datapath, listDir[i])
        if os.path.isfile(filePath):
            if u'住院' in listDir[i]:
                settleData = pd.read_csv(filePath, encoding='gb18030', dtype={u'就诊登记号': np.str})
            else:
                data = pd.read_csv(filePath, encoding='gb18030',
                                   dtype={u'药品剂型': np.str, u'分类名称': np.str, u'中心端名称': np.str, u'就诊登记号': np.str,
                                          u'明细发生时间': np.str})
    log.info('Read rawData end')
    # TODO 分析数据
    log.info('Data analyse begin')
    data = readData(data)
    data = data.fillna('nan')

    #某天的明细是否只包括床位、护理、其他诊疗，是取1，非取0
    df = data['category'].groupby([data['clinid'],data['feetime']]).apply(flag).reset_index()
    df.rename(columns={'category':'flag'},inplace=True)

    # 查看连续7天以上没有发生实质性治疗的
    df_1 = df[['flag','feetime']].groupby(df['clinid']).apply(findMaxConsecutiveOnes).reset_index()
    df_1.columns = ['clinid','flagtime']
    df_1['endTime'] = df_1.flagtime.apply(lambda x: x[2])
    df_1['startTime'] = df_1.flagtime.apply(lambda x: x[1])
    df_1['flag'] = df_1.flagtime.apply(lambda x: x[0])
    df_1 = df_1[['clinid','startTime','endTime','flag']]

    df_2 = df_1[df_1['flag'] > 7]
    df_2 = df_2.sort_values(by=['flag'], ascending=[False])
    print df_2.head()

    #输出结果
    dflist = []
    idlist = []
    for i in df_2['clinid'].values:
        output = data[data['clinid'] == str(i)]
        output['flag'] = list(df_2[df_2['clinid']==i]['flag'])[0]
        output['startTime'] = list(df_2[df_2['clinid'] == i]['startTime'])[0]
        output['endTime'] = list(df_2[df_2['clinid'] == i]['endTime'])[0]
        output['days'] = output['outtime']- output['intime']
        output['days'] = (output['days']/np.timedelta64(1,'D')).astype(int)

        outputdata = pd.merge(output, settleData,left_on='clinid',right_on=u'就诊登记号',how='left' )
        outputdata= outputdata.fillna('nan')
        outputdata[u'报销金额'] = outputdata[u'报销金额'].astype(float)
        outputdata['claimRatio'] = (outputdata[u'报销金额'] / outputdata[u'医疗总费用']).astype(float)
        if list(outputdata['claimRatio'])[1]>0.5:
            dflist.append(outputdata)
    if len(dflist)!= 0:
        result = pd.concat(dflist)
        result['department'] = ''
        result['doctor'] = ''

    # TODO 输出
    filename1 = u'\\su_illegal_stay_hospital.csv'
    filename2 = u'\\su_illegal_stay_hospital_detail.csv'
    outputfile1 = resultPath + filename1
    outputfile2 = resultPath + filename2
    # #主表输出
    su_illegal_stay_hospital = result.drop_duplicates(subset=['clinid'])
    su_illegal_stay_hospital = su_illegal_stay_hospital[['clinid', u'个人编码', u'患者姓名_x', u'患者身份证_x',
    u'医院所在分中心_x', u'医院名称_x', u'医院等级_x', 'department', 'doctor', 'intime', 'outtime', 'days', 'startTime'
    , 'endTime', 'flag', u'医疗总费用', u'报销金额']]
    su_illegal_stay_hospital.rename(columns={'clinid':'jzId',u'个人编码':'sbId',u'患者姓名_x':'patientName',u'患者身份证_x':'idcard',
     u'医院所在分中心_x':'qxName',u'医院名称_x':'hospitalName',u'医院等级_x':'hospitalLevel','department':'department',
    'doctor':'doctorName', 'intime':'inHosTime','outtime': 'outHosTime', 'outtime':'outHosTime','days':'inHosDays',
     'startTime':'stayStartTime','endTime':'stayEndTime','flag':'stayDays',u'医疗总费用':'allMoney', u'报销金额':'bcMoney'},inplace=True)
    su_illegal_stay_hospital.drop_duplicates(subset=['jzId'],inplace=True)
    su_illegal_stay_hospital.to_csv(outputfile1, encoding='gb18030', index=False)
    # to database
    log.info('Creat database engine')
    # engine = create_engine('mysql+mysqldb://%s:%s@%s/%s?charset=utf8' % (username, password, tns, databasename))
    # su_illegal_stay_hospital.to_sql(table, con=engine, if_exists='append', index=False)

    #明细表输出
    su_illegal_stay_hospital_detail = result[['clinid',u'医保编码',u'中心端名称',u'医院端名称',u'明细总额',u'明细单价',u'明细数量',u'明细发生时间']]
    idlist = []
    for i in range(len(su_illegal_stay_hospital_detail)):
        idlist.append(shortuuid.uuid())
    su_illegal_stay_hospital_detail['id'] = idlist
    su_illegal_stay_hospital_detail.rename(columns={'clinid':'jzId',u'医保编码':'centerCode',u'中心端名称':'centerName',u'医院端名称':'hospitalEndName',
                                            u'明细总额':'totalMoney',u'明细单价':'unitPrice',u'明细数量':'quantity',u'明细发生时间':'createTime'},inplace=True)
    su_illegal_stay_hospital_detail['createTime']=su_illegal_stay_hospital_detail['createTime'].apply(lambda x: pd.to_datetime(x).date())
    su_illegal_stay_hospital_detail.to_csv(outputfile2, encoding='gb18030',index=False)
    # su_illegal_stay_hospital_detail.to_sql(tabledetail, con=engine, if_exists='append', index=False)

