# -*- coding:utf8*-

"""
Created on Tue Jan 10 16:55:42 2017

@author: Administrator
"""

import pandas as pd
import numpy as np
import re
import json



path = './data/programNumAnomaly_2015.csv'
diseasename = ['I25', 'I10', 'I63', 'N19', 'C34']


def readdata(path):
	data = pd.read_csv(path, encoding='gb18030')
	column_names = {'医院名称': 'hospital', '医院等级': 'hos_lev', '住院序号': 'hos_id', '姓名': 'pat_name', '身份证': 'Id_card',
					'年龄': 'age', '工作单位': 'work',
					'疾病编码': 'disease_id', '疾病名称': 'diseasename', '入院时间': 'intime', '出院时间': 'outtime', '住院天数': 'day',
					'费用时间': 'ontime',
					'医保中心项目编码': 'program_id', '项目名称': 'program_name', '单价': 'program_price', '数量': 'program_num', '总费用': 'program_cost',
					'自付比例': 'zifubili', '医保范围费用': 'yibaofanweifeiyong', '统筹范围费用': 'tongcoufanweifeiyong',
					'自付费用': 'zifufeiyong',
					'自理费用': 'zilifeiyong','医院码':'yiyuanma'}
	data = data.rename(columns=column_names)
	data['disease'] = data['disease_id'].str.extract('(\D\d\d)')
	return data


def disease(data):
	for i in diseasename:
		print('current process disease:' + i)
		curDiseaseData = data[data['disease'] == i]
		prepareedData = datapreprocessing(curDiseaseData)
		finalResultDf,freqenceDf = dataanalyse(prepareedData)
		output(finalResultDf,freqenceDf, i)
		print(finalResultDf.head())
		break

def datapreprocessing(curDiseaseData):
	# 删除药品以及yy开头的材料费用
	curDiseaseData['new'] = curDiseaseData['program_id'].apply(lambda x: bool(re.match("^[a-zA-Z]", str(x))))
	curDiseaseData = curDiseaseData[curDiseaseData['new'] == True]
	curDiseaseData = curDiseaseData.drop('new', axis='columns')
	curDiseaseData = curDiseaseData[curDiseaseData['program_id'] != 'AA0929']
	curDiseaseData['cailiao'] = curDiseaseData['program_id'].apply(lambda x: bool(re.match("YY", str(x))))
	curDiseaseData = curDiseaseData[curDiseaseData['cailiao'] == False]
	curDiseaseData = curDiseaseData.drop('cailiao', axis='columns')
	# 去掉包含抢救的
	curDiseaseData['qiangjiu'] = curDiseaseData['program_name'].apply(lambda x: bool(re.match(".抢救", str(x))))
	curDiseaseData = curDiseaseData[curDiseaseData['qiangjiu'] == False]
	curDiseaseData = curDiseaseData.drop('qiangjiu', axis='columns')
	# 去除价格小于20的检查
	df = curDiseaseData[curDiseaseData['program_price'] > 20]
	return df


def dataanalyse(df):
	# 计算平均每天检查次数（住院期间）
	df['day'].replace(0, 1, inplace=True)
	df['program_per_num'] = df['program_num'] / df['day']
	# 计算检查次数的均值和每天检查次数的均值
	freqenceSerials=df.groupby(['program_id','program_num']).size()
	freqenceDf=pd.DataFrame(freqenceSerials).reset_index()
	mean = df['program_num'].groupby(df['program_id']).mean().reset_index()
	df_1 = pd.merge(df, mean, left_on=['program_id'], right_on=['program_id'], how='left')
	df_1 = df_1.rename(columns={'program_num_x': 'program_num', 'program_num_y': 'program_num_mean'})
	mean_d = df['program_per_num'].groupby(df['program_id']).mean().reset_index()
	df_2 = pd.merge(df_1, mean_d, left_on=['program_id'], right_on=['program_id'], how='left')
	df_2 = df_2.rename(columns={'program_per_num_x': 'program_per_num', 'program_per_num_y': 'program_num_d_mean'})
	# 计算离群点（四分位距）
	box_program_num = df_2['program_num'].groupby(df_2['program_id']).apply(
		lambda x: np.percentile(x, 75) + 1.5 * (np.percentile(x, 75) - np.percentile(x, 25))).reset_index()
	df_3 = pd.merge(df_2, box_program_num, left_on=['program_id'], right_on=['program_id'], how='left')
	df_3 = df_3.rename(columns={'program_num_y': 'box_program_num', 'program_num_x': 'program_num'})

	box_program_per_num = df_3['program_per_num'].groupby(df_3['program_id']).apply(
		lambda x: np.percentile(x, 75) + 1.5 * (np.percentile(x, 75) - np.percentile(x, 25))).reset_index()
	df_3 = pd.merge(df_3, box_program_per_num, left_on=['program_id'], right_on=['program_id'], how='left')
	df_3 = df_3.rename(columns={'program_per_num_x': 'program_per_num', 'program_per_num_y': 'box_program_per_num'})

	bylev = df.groupby(['program_id', 'hos_lev'])
	box_hos_program_per_num = bylev['program_per_num'].apply(
		lambda x: np.percentile(x, 75) + 1.5 * (np.percentile(x, 75) - np.percentile(x, 25))).reset_index()
	df_3 = pd.merge(df_3, box_hos_program_per_num, left_on=['program_id', 'hos_lev'], right_on=['program_id', 'hos_lev'],
					how='left')
	df_3 = df_3.rename(columns={'program_per_num_x': 'program_per_num', 'program_per_num_y': 'box_hos_program_per_num'})

	bylev = df_3.groupby(['program_id', 'hos_lev'])
	box_hos_program_num = bylev['program_num'].apply(
		lambda x: np.percentile(x, 75) + 1.5 * (np.percentile(x, 75) - np.percentile(x, 25))).reset_index()
	df_3 = pd.merge(df_3, box_hos_program_num, left_on=['program_id', 'hos_lev'], right_on=['program_id', 'hos_lev'], how='left')
	df_3 = df_3.rename(columns={'program_num_x': 'program_num', 'program_num_y': 'box_hos_program_num'})

	df_3['outerlier_iqr'] = df_3['program_num'] > df_3['box_program_num']
	df_3['outerlier_iqr_d'] = df_3['program_per_num'] > df_3['box_program_per_num']
	df_3['outerlier_iqr_hoslev'] = df_3['program_num'] > df_3['box_hos_program_num']
	df_3['outerlier_iqr_d_hoslev'] = df_3['program_num'] > df_3['box_hos_program_per_num']
	# 计算检查次数program_num与99.3%人的检查次数间隔大于2的
	df_3['intervel'] = df_3['program_num'] - df_3['box_program_num']
	df_4 = df_3[df_3['intervel'] > 2]
	# 计算指标都超过的
	df_4 = df_4[
		(df_4['outerlier_iqr'] == True) & (df_4['outerlier_iqr_d'] == True) & (df_4['outerlier_iqr_hoslev'] == True) & (
		df_4['outerlier_iqr_d_hoslev'] == True)]
	df_4 = df_4.drop('disease', axis='columns')
	return df_4,freqenceDf

def output(df_4,freqenceDf,i):
	print(freqenceDf.head())
	df_4.sort_values(by=['program_id','program_num'],ascending=[False,False],inplace=True)
	pd.set_option('display.max_columns', None)
	column_names={'hos_name':'医院名称','hos_lev':'医院等级','hos_id':'住院序号','pat_name':'姓名','personid':'身份证','age':'年龄','work':'工作单位',
             'disease_id':'疾病编码','diseasename':'疾病名称','intime':'入院时间','outtime':'出院时间','day':'住院天数','ontime':'费用时间',
              'program_id':'医保中心项目编码','program_name':'项目名称','price':'单价','program_num':'检查次数','totalcost':'总费用',
              'zifubili':'自付比例','yibaofanweifeiyong':'医保范围费用', 'tongcoufanweifeiyong':'统筹范围费用','zifufeiyong':'自付费用',
              'zilifeiyong':'自理费用','program_per_num':'每天检查次数','box_program_num':'99.3%患者检查次数小于','box_program_per_num':'99.3%患者每天检查次数小于',
             'box_hos_program_num':'同级别医院，99.3%患者检查次数小于','box_hos_program_per_num':'同级别医院，99.3%患者每天检查次数小于','program_num_mean':'平均检查',
             'program_num_d_mean':'平均日均检查',}
	for eachRow in df_4.iterrows():
		jsonMap={"type":u"项目数量异常","common":None,"tabs":[]}
		print("begin")
		commonParar={}
		for index in range(0,len(df_4.columns)):
			commonParar[df_4.columns[index]]=str(eachRow[1][index])
		# jsonResult = json.dumps(commonParar)
		jsonMap["common"]=commonParar
		#frequence figure
		frequenceFigureMap={"echartsType": "bar", "echartsUnit": "%", "name": u"频次分布图", "data": []}

		programFreDf= freqenceDf[freqenceDf.program_id==commonParar['program_id']]
		tableArray=[]
		for eachRow in programFreDf.iterrows():
			tableMap={}
			tableMap["program_num"]=eachRow[1][1]
			tableMap["frequence"]=eachRow[1][2]
			tableArray.append(tableMap)
		frequenceFigureMap["data"]=tableArray

		jsonMap["tabs"].append(frequenceFigureMap)

		finalResult=json.dumps(jsonMap)
		print(finalResult)

		f=open("d://programeJson.txt",'w')
		f.write(finalResult)
		f.close()

		jsonResult = json.dumps(tableArray)
		print(jsonResult)
		break



	df_5=df_4.rename(columns=column_names)


	df_5.to_csv('E:/数联易康/2016-12/德阳项目反馈/医疗浪费/'+i+'_2015x'+'.csv')

if __name__ == '__main__':
	soruceData = readdata(path)
	disease(soruceData)
