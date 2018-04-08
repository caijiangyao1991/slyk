# -*- coding:utf8*-

"""
Created on Tue Jan 11 16:55:42 2017

@author: Administrator
"""
import pandas as pd
import numpy as np
import re
import json
import pandas as pd
import numpy as np
import hashlib
import xlrd
import xlwt


path = './data/hosDrugGroupAbuse_2015.csv'
diseasename = ['J44','I10']
def readdata(path):
	data = pd.read_csv( path, encoding='gb18030')
	data['intime']=pd.to_datetime(data['intime'])
	data.index=data['intime']
	data=data.drop('intime',axis='columns')
	data_2015=data['2015']
	indexs=list(range(len(data_2015)))
	data_2015.index=indexs
	df = data_2015[['hos_ma', 'hos_name', 'hos_lev', 'disease_id', 'disease_name', 'hos_id', 'drug_group_id', 'drug_group_name','totalcost','yibaofanweifeiyong']]
	df['disease'] = df['disease_id'].str.extract('(\D\d\d)')
	return df

def disease(df):

	outfile = "d://drugGroup.xls"
	xls = xlwt.Workbook()
	for disease in diseasename:
		curDiseaseData = df[df['disease'] == disease]
		prepareedData = datapreprocessing(curDiseaseData)
		output(prepareedData,curDiseaseData,disease,xls)
		print(disease)
	xls.save(outfile)

def datapreprocessing(curDiseaseData):
	curDiseaseData = curDiseaseData.drop(['totalcost','yibaofanweifeiyong'], axis='columns')
	binli = curDiseaseData.drop_duplicates(subset='hos_id')
	hos_num = binli['hos_id'].groupby(binli['hos_ma']).count().reset_index()
	df_1 = pd.merge(curDiseaseData, hos_num, left_on=['hos_ma'], right_on=['hos_ma'], how='left')
	df_1 = df_1.rename(columns={'hos_id_x': 'hos_id', 'hos_id_y': 'hos_num'})
	df_1 = df_1[df_1['hos_num'] > 50]
	df_1 = df_1.drop(['hos_num'], axis='columns')
	df_2 = df_1[df_1['drug_group_id'].isnull() == False]
	df_2['fuzhulie'] = 1
	by_drug = df_2.pivot(index='hos_id', columns='drug_group_id', values='fuzhulie')
	by_drug = by_drug.fillna(0).reset_index()
	df_3 = df_2.drop_duplicates(subset='hos_id')
	df_4 = pd.merge(df_3, by_drug, left_on=['hos_id'], right_on=['hos_id'], how='left')
	df_4 = df_4.drop(['fuzhulie'], axis='columns')
	df_5 = df_4.drop(['hos_lev', 'disease_id', 'disease_name', 'hos_name', 'drug_group_id', 'drug_group_name', 'hos_id'],axis='columns')
	by_hos_count = df_5.groupby('hos_ma').count().reset_index()
	by_hos_sum = df_5.groupby('hos_ma').sum().reset_index()
	df_6 = by_hos_count
	df_6.ix[:, 1:] = by_hos_sum.ix[:, 1:] / by_hos_count.ix[:, 1:]
	df_lev = df_4[['hos_lev', 'hos_ma']]
	df_lev = df_lev.drop_duplicates(subset='hos_ma')
	df_7 = pd.merge(df_6, df_lev, left_on=['hos_ma'], right_on=['hos_ma'], how='left')
	df_hos = df_2[['hos_ma', 'hos_name', 'hos_lev']]
	df_hos = df_hos.drop_duplicates(subset='hos_ma')
	df_7 = pd.merge(df_7, df_hos, left_on=['hos_ma'], right_on=['hos_ma'], how='left')
	df_7.drop(['hos_ma', 'hos_lev_x', 'hos_lev_y'], axis='columns', inplace=True)
	df_7_1 = df_7.ix[:, 0:-1]

	df_list_max = []
	for i in df_7_1.columns:
		df = df_7[i].max()
		df_list_max.append(df)

	df_list_mean = []
	for i in df_7_1.columns:
		df = df_7[i].mean()
		df_list_mean.append(df)

	df_list_maxmean = []
	for i in df_7_1.columns:
		df = df_7[i].max() - df_7[i].mean()
		df_list_maxmean.append(df)

	df_8 = df_7.set_index('hos_name').T
	df_8['maxmean'] = df_list_maxmean
	df_8['max'] = df_list_max
	df_8['mean'] = df_list_mean

	df_9 = df_8.reset_index()
	df_drug = df_2[['drug_group_id', 'drug_group_name']]
	df_drug = df_drug.drop_duplicates(subset='drug_group_id')
	df_10 = pd.merge(df_9, df_drug, left_on=['index'], right_on=['drug_group_id'])
	df_10 = df_10.drop('index', axis='columns')
	df_10 = df_10.set_index('drug_group_id')

	df_11 = df_10.T
	df_11 = df_11.reset_index()
	df_12 = df_11.set_index('hos_name')
	df_13 = df_12.T
	df_14 = df_13[(df_13['maxmean'] > 0.5)]
	return df_14

def output(df_14,curDiseaseData,disease,xls):
	jsonMap = {"type": u"医院药品组滥用", "common": None, "tabs": []}
	ratioFigureMap = {"echartsType": "bar", "echartsUnit": "%", "name": u"异常分布图", "data": []}

	sheet = xls.add_sheet(str(disease)+'sample')
	for j in range(len(df_14)):
		tableArray = []
		for i in range(len(df_14.columns) - 4):
			tableMap = {}
			tableMap['hospitalName'] = str(df_14.columns[i])
			tableMap['ratio'] = str(df_14.ix[j][i])
			hosnamei = str(df_14.columns[i])
			hosmai = str(list(curDiseaseData[curDiseaseData['hos_name'] == hosnamei]['hos_ma'])[0])
			druggroupnamei = str(df_14.ix[j]['drug_group_name'])
			druggroupidi = str(list(curDiseaseData[curDiseaseData['drug_group_name'] == druggroupnamei]['drug_group_id'])[0])
			curDiseaseDataHosi = curDiseaseData[curDiseaseData['hos_ma'] == hosmai]
			tableMap['curDiseasePersonNum'] = str(len(set(curDiseaseDataHosi['hos_id'])))
			tableMap['curDiseaseProgramPersonNum'] = str(len(set(curDiseaseDataHosi[curDiseaseDataHosi['drug_group_id'] == druggroupidi]['hos_id'])))
			tableArray.append(tableMap)
			if df_14.ix[j][i] == list(df_14['max'])[j]:
				commonParar = {}
				commonParar['drugGroupName'] = str(df_14.ix[j]['drug_group_name'])
				commonParar['curDrugGroupRatio'] = str(df_14.ix[j][i])
				commonParar['avgDrugGroupRatio'] = str(df_14.ix[j]['mean'])
				commonParar['diseaseName'] = "其他慢性阻塞性肺病"
				commonParar['hospitalName'] = str(df_14.columns[i])
				commonParar['anomalyYear'] = '2015'


                #求医院名字和医院等级和医院码
				hosname = str(df_14.columns[i])
				hoslevel = str(list(curDiseaseData[curDiseaseData['hos_name'] == str(df_14.columns[i])]['hos_lev'])[0])
				hosma = str(list(curDiseaseData[curDiseaseData['hos_name'] == str(df_14.columns[i])]['hos_ma'])[0])

				#求总费用和报销费用
				drug_group_id=list(curDiseaseData[curDiseaseData['drug_group_name']== str(df_14.ix[j]['drug_group_name']) ]['drug_group_id'])[0]
				curDiseaseDataHos=curDiseaseData[curDiseaseData['hos_ma']==hosma]
				totalCost = curDiseaseDataHos['totalcost'].groupby(curDiseaseDataHos['drug_group_id']).sum()
				claimCost = curDiseaseDataHos['yibaofanweifeiyong'].groupby(curDiseaseDataHos['drug_group_id']).sum()
				totalCostValue = list(totalCost[totalCost.index == drug_group_id])[0]
				claimCostValue = list(claimCost[claimCost.index == drug_group_id])[0]

				# 求药品组码
				druggroupname = str(df_14.ix[j]['drug_group_name'])
				druggroupid = str(list(curDiseaseData[curDiseaseData['drug_group_name'] == druggroupname]['drug_group_id'])[0])

				#求该病种下的住院人次和该病种下做该项目的住院人次
				commonParar['curDiseasePersonNum'] = len(set(curDiseaseDataHos['hos_id']))
				commonParar['curDiseaseProgramPersonNum'] = len(set(curDiseaseDataHos[curDiseaseDataHos['drug_group_id'] == druggroupid]['hos_id']))


		ratioFigureMap['data'] = tableArray
		jsonMap['common'] = commonParar
		jsonMap['tabs'].append(ratioFigureMap)
		finalResult = json.dumps(jsonMap,ensure_ascii=False)

		src = finalResult
		finalMD5 = hashlib.md5(src.encode(encoding='gb2312'))

		sheet.write(j, 9, str(totalCostValue))
		sheet.write(j, 10, str(claimCostValue))
		sheet.write(j, 13, hosma)
		sheet.write(j, 14, hosname)
		sheet.write(j, 20, hoslevel)
		sheet.write(j, 23, finalResult)
		sheet.write(j, 24, str(finalMD5))
		jsonMap['tabs'] = []
		ratioFigureMap = {"echartsType": "bar", "echartsUnit": "%", "name": u"异常分布图", "data": []}
		jsonMap = {"type": u"医院药品组滥用", "common": None, "tabs": []}


		# print(finalResult)
		# f = open('d://drugGroupJson'+str(j)+'.txt', 'w')
		# f.write(finalResult)
		# f.close()

		# print(str(finalMD5))
		# f2 = open('d://drugGroupMd5' + str(j) + '.txt', 'w')
		# f2.write(str(finalMD5))
		# f2.close()


if __name__ == '__main__':
	soruceData = readdata(path)
	disease(soruceData)











