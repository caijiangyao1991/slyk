# coding=utf-8
from tool.DBAcess.postgre import DBAccess
import pandas as pd
import numpy as np
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

#以药品中文名作为唯一ID

def readdata():
    # db = DBAccess('pzh')
    # df = db.get_db_df(
    #     "select visit_ordr_num, pharmacy_id,pharmacy_name, med_cd, comn_fst_nm_nm, spec,indv_id ,indv_name, exam_post_uprc, exam_post_qty, exam_post_amt from t_ydskmx")
    df = pd.read_csv('./data/heaCareAbuse_drugstore.csv',encoding='gb18030')
    return df


def dataanalyse(df):
    # 计算每家药店销售的总次数
    order = df.groupby([df['pharmacy_id'],df['visit_ordr_num']]).size().reset_index()
    orderSum = order['visit_ordr_num'].groupby(df['pharmacy_id']).count().reset_index()
    orderSum.rename(columns={0:'orderSum'}, inplace=True)

    # 计算每个药 在每家药店卖的次数
    drug = df.groupby([df['pharmacy_id'],df['comn_fst_nm_nm']]).size().reset_index()
    drug.rename(columns={0: 'durgSum'}, inplace=True)
    print(len(drug))

    df_1 = pd.merge(drug,orderSum, left_on='pharmacy_id',right_on='pharmacy_id', how='left')
    print(len(df_1))

    #计算平均价格>200的药
    price = df['exam_post_uprc'].groupby(df['comn_fst_nm_nm']).mean().reset_index()


    df_2 = df_1[df_1['comn_fst_nm_nm'].isin(price[price['exam_post_uprc']>200]['comn_fst_nm_nm'])]
    df_2['ratio'] = df_2['durgSum']/df_2['orderSum']


    df_3 = df_2.pivot(index='pharmacy_id', columns='comn_fst_nm_nm', values='ratio')


    df_list_max = []
    for i in df_3.columns:
        df = df_3[i].max()
        df_list_max.append(df)

    df_4 = df_2.pivot(index='comn_fst_nm_nm', columns='pharmacy_id', values='ratio')
    df_4['maxratio'] = df_list_max
    df_5 = df_4[df_4['maxratio']>0.1]
    for i in range(len(df_5)):
        for j in df_5.columns[:-1]:
            if (df_5[j][i] == df_5['maxratio'][i]):
                print(j)
                print(df_5['maxratio'][i])
                print(df_5.index[i])



    # df_3.to_csv(u'E:/数联易康/2017-06/保健品销量情况/result.csv',encoding='gb18030')





if __name__ == '__main__':
    rawdata = readdata()
    dataanalyse(rawdata)

