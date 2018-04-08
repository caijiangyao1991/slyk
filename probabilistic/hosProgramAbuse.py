# -*- coding:gbk*-

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

year='2015'
path = './data/hosProgramAbuse_2015.csv'
diseasename = ['J44','I25', 'I10', 'I63', 'N19', 'C34']
MinPeople=15

def readdata(path):
	data = pd.read_csv(path, encoding='gb18030')
	column_names = {'ҽԺ��':'hos_ma','ҽԺ����': 'hos_name', 'ҽԺ�ȼ�': 'hos_lev', 'סԺ���': 'hos_id', '����': 'pat_name', '���֤': 'personid',
					'����': 'age', '������λ': 'work',
					'��������': 'disease_id', '��������': 'diseasename', '��Ժʱ��': 'intime', '��Ժʱ��': 'outtime', 'סԺ����': 'day',
					'����ʱ��': 'ontime',
					'ҽ��������Ŀ����': 'program_id', '��Ŀ����': 'program_name', '����': 'price', '����': 'number', '�ܷ���': 'totalcost',
					'�Ը�����': 'zifubili', 'ҽ����Χ����': 'yibaofanweifeiyong', 'ͳ�ﷶΧ����': 'tongcoufanweifeiyong',
					'�Ը�����': 'zifufeiyong',
					'�������': 'zilifeiyong'}
	data = data.rename(columns=column_names)
	df = data[['hos_ma', 'hos_name', 'hos_lev', 'disease_id', 'diseasename', 'hos_id', 'program_id', 'program_name', 'price']]
	df['disease'] = data['disease_id'].str.extract('(\D\d\d)')
	return df

def disease(df):
	levels=[1]
	levelResultMap={}

	for disease in diseasename:
		for eachLevel in levels:
			levelResultMap[eachLevel] = None
		# print('current process disease:' + i)
		curDiseaseData = df[df['disease'] == disease]
		prepareedData = datapreprocessing(curDiseaseData)
		analysAllLevel(prepareedData, levelResultMap)

		for eachLevel in levels:
			curLevelRs=levelResultMap[eachLevel]
			if levelResultMap[eachLevel] is not None:
				output(curLevelRs,  disease,eachLevel)

def datapreprocessing(curDiseaseData):
	# ɾ��ҽԺ������С��50��ҽԺ
	binli = curDiseaseData.drop_duplicates(subset='hos_id')
	hos_num = binli['hos_id'].groupby(binli['hos_ma']).count().reset_index()
	df_1 = pd.merge(curDiseaseData, hos_num, left_on=['hos_ma'], right_on=['hos_ma'], how='left')
	df_1.rename(columns={'hos_id_x': 'hos_id', 'hos_id_y': 'hos_num'},inplace=True)
	df_1 = df_1[df_1['hos_num'] > MinPeople]
	df_1.drop(['hos_num'], axis='columns',inplace=True)
	# ȥ���۸�С��20�ļ��
	df_2 = df_1[df_1['price'] > 20]
	# ɾ��ҩƷ�Լ�yy��ͷ�Ĳ��Ϸ���
	df_2['new'] = df_2['program_id'].apply(lambda x: bool(re.match("^[a-zA-Z]", str(x))))
	df_2 = df_2[df_2['new'] == True]
	df_2.drop('new', axis='columns',inplace=True)
	df_2 = df_2[df_2['program_id'] != 'AA0929']
	df_2['cailiao'] = df_2['program_id'].apply(lambda x: bool(re.match("YY", str(x))))
	df_2 = df_2[df_2['cailiao'] == False]
	df_2.drop('cailiao', axis='columns',inplace=True)
	return df_2


def analysAllLevel(df_2,levelResultMap):

	for eachLevel in levelResultMap.keys():
		rsDf = dataanalyse(df_2, eachLevel)
		if rsDf is None:
			continue

		levelResultMap[eachLevel]=rsDf

def dataanalyse(df_2,level):
	#��ҽԺ�������ݷ���
	df_lev1_2=df_2
	# df_lev1_2 = df_2[df_2['hos_lev'] == level]
	# if len(df_lev1_2) == 0:
	# 	return None
	# hos_number = df_lev1_2['hos_ma'].drop_duplicates('hos_ma',keep='first')
	idSet=set()
	for each in df_lev1_2['hos_ma']:
		idSet.add(each)
	# for each in idSet:
	# 	print(each)

	if len(idSet)<5:
		print('hos_number<5')
		return None


	df_lev1_2['fuzhulie'] = 1
	by_program = df_lev1_2.pivot(index='hos_id', columns='program_id', values='fuzhulie')
	by_program = by_program.fillna(0).reset_index()
	df_lev1_3 = df_lev1_2.drop_duplicates(subset='hos_id')
	df_lev1_4 = pd.merge(df_lev1_3, by_program, left_on=['hos_id'], right_on=['hos_id'], how='left')
	df_lev1_4.drop(['fuzhulie', 'price'], axis='columns', inplace=True)
	df_lev1_5 = df_lev1_4.drop(['hos_lev', 'disease_id', 'diseasename', 'hos_name', 'program_id', 'program_name', 'hos_id'], axis='columns')

	by_hos_sum = df_lev1_5.groupby('hos_ma').sum().reset_index()
	by_hos_count = df_lev1_5.groupby('hos_ma').count().reset_index()
	df_lev1_6 = by_hos_count
	df_lev1_6.ix[:, 1:] = by_hos_sum.ix[:, 1:] / by_hos_count.ix[:, 1:]
	lev = df_lev1_4[['hos_lev', 'hos_ma']]
	lev = lev.drop_duplicates(subset='hos_ma')
	df_lev1_7 = pd.merge(df_lev1_6, lev, left_on=['hos_ma'], right_on=['hos_ma'], how='left')

	df_hos = df_lev1_2[['hos_ma', 'hos_name']]
	df_hos = df_hos.drop_duplicates(subset='hos_ma')
	df_lev1_7 = pd.merge(df_lev1_7, df_hos, left_on=['hos_ma'], right_on=['hos_ma'], how='left')
	df_lev1_7.drop('hos_ma', axis='columns', inplace=True)
	df_lev1_7 = df_lev1_7.set_index('hos_name')

	df_list_mean = []
	for i in df_lev1_7.columns:
		df = df_lev1_7[i].mean()
		df_list_mean.append(df)
	df_list_std = []
	for i in df_lev1_7.columns:
		df = df_lev1_7[i].std()
		df_list_std.append(df)
	df_list_maxmin = []
	for i in df_lev1_7.columns:
		df = df_lev1_7[i].max() - df_lev1_7[i].min()
		df_list_maxmin.append(df)
	df_lev1_8 = df_lev1_7.T

	df_lev1_8['mean'] = df_list_mean
	df_lev1_8['std'] = df_list_std
	df_lev1_8['maxmin'] = df_list_maxmin

	df_lev1_9 = df_lev1_8.reset_index('hos_ma')
	df_program = df_lev1_2[['program_id', 'program_name']]
	df_program = df_program.drop_duplicates(subset='program_id')
	df_lev1_10 = pd.merge(df_lev1_9, df_program, left_on=['index'], right_on=['program_id'])
	df_lev1_10 = df_lev1_10.drop('index', axis='columns')

	df_lev1_11 = df_lev1_10.set_index(['program_id'])
	df_lev1_11['bianyixishu'] = df_lev1_11['std'] / df_lev1_11['mean']




	threadHold_std= np.percentile(list(df_lev1_11['std']), 95)
	threadHold_byxs= np.percentile(list(df_lev1_11['bianyixishu']), 95)

	df_lev1_12 = df_lev1_11[(df_lev1_11['maxmin'] > 0.5) &
							((df_lev1_11['std'] >threadHold_std) |
							 (df_lev1_11['bianyixishu'] > threadHold_byxs)
							 )]
	return df_lev1_12

def output(df_lev1_12,disease,level):
	jsonMap = {"type": u"ҽԺ��Ŀ����", "common": None, "tabs": []}
	ratioFigureMap = {"echartsType": "bar", "echartsUnit": "%", "name": u"�쳣�ֲ�ͼ", "data": []}

	for j in range(len(df_lev1_12)):
		tableArray = []
		for i in range(len(df_lev1_12.columns) - 5):
			tableMap = {}
			tableMap['hospital'] = str(df_lev1_12.columns[i])
			tableMap['ratio'] = str(df_lev1_12.ix[j][i])
			tableArray.append(tableMap)
			if df_lev1_12.ix[j][i] > 0.5:
				# print(df_lev1_12.columns[i])
				# print(df_lev1_12.ix[j]['program_name'])
				# print(df_lev1_12.ix[j][i])
				commonParar= {}
				commonParar['program_name'] = str(df_lev1_12.ix[j]['program_name'])
				commonParar['cur_ratio'] = str(df_lev1_12.ix[j][i])
				commonParar['avg_program_ratio'] = str(df_lev1_12.ix[j]['mean'])
				commonParar['year'] = str(year)
				commonParar['disease_type'] = str(disease)


		ratioFigureMap['data'] = tableArray
		jsonMap['common'] = commonParar
		jsonMap['tabs'].append(ratioFigureMap)
		finalResult=json.dumps(jsonMap)
		print(finalResult)

		f = open('d://jianchaJson.txt', 'w')
		f.write(finalResult)
		f.close()
		break



	# df_lev1_12.to_excel('E:/�����׿�/2017-01/������Ŀ����/ҩƷ������쳣/����쳣����/' + str(disease) +'_2015.xlsx')


if __name__ == '__main__':
	soruceData = readdata(path)
	disease(soruceData)