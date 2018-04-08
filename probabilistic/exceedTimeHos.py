import json
import pandas as pd
import numpy as np

bins = [-np.inf, 56, 65, 75, np.inf]
age_bin = ['(-inf, 45]', '(45, 60]', '(60, 75]', '(75, inf]']
path = './data/exceedTimeHos.csv'
age_levels = [('(-inf, 45]',1), ('(45, 60]', 1), ('(60, 75]', 1), ('(75, inf]', 1),('(-inf, 45]', 2), ('(45, 60]', 2), ('(60, 75]', 2), ('(75, inf]', 2),('(-inf, 45]', 3), ('(45, 60]', 3), ('(60, 75]', 3), ('(75, inf]', 3)]
diseasename = ['J44', 'I25', 'I10', 'I63', 'N19', 'C34']


def readdata(path):
	data = pd.read_csv(path, encoding='gb18030')
	column_names = {'a3': 'id', 'a4': 'hos_id', 'a5': 'hos_name', 'a5.1': 'hos_level', 'a6': 'timein', 'a7': 'timeout',
					'a8': 'diseasetype', 'a9': 'diseasename', 'a13': 'pat_name', 'a14': 'Id_card', 'a16': 'company',
					'a18': 'drug_sumcost','a19': 'program_sumcost','a31': 'gender','a32': 'age'}
	data = data.rename(columns=column_names)
	data['hos_intervel']=data.filter(regex=("time.*")).apply(lambda x: ''.join(str(x.values)), axis=1)

	data['in'] = data.timein.apply(lambda x: pd.to_datetime(x).date())
	data['out'] = data.timeout.apply(lambda x: pd.to_datetime(x).date())
	data['day'] = data['out'] - data['in']
	data['day'] = (data['day'] / np.timedelta64(1, 'D')).astype(int)

	data['disease_3'] = data['diseasetype'].str.extract('(\D\d\d)')
	data = data[data['disease_3'].isnull() == False]
	data['age_bin'] = pd.cut(data['age'], bins)
	return data


def dataalyse(data):
	for i in diseasename:
		curDiseaseData = data[data['disease_3'] == i]
		for j, h in age_levels:
			curData = curDiseaseData[(curDiseaseData['age_bin'] == j) & (curDiseaseData['hos_level'] == h)]
			freqenceSerials = curData.groupby(['day']).size()
			freqenceDf = pd.DataFrame(freqenceSerials).reset_index()
			print(freqenceDf.head())
			curData['threeSigema_days'] = curData['day'].mean() + 3 * curData['day'].std()
			curData['avg_days'] = curData['day'].mean()
			curData['outliers'] = (curData['day'] > curData['threeSigema_days'])
			finalData = curData[curData['outliers'] == True]
	return (finalData, freqenceDf, i, j, h)


def output(finalData, freqenceDf, i, j, h):
	for eachRow in finalData.iterrows():
		jsonMap = {"type": u"超期住院", "common": None, "tabs": []}
		commonParar = {}
		for index in range(0, len(finalData.columns)):
			commonParar[finalData.columns[index]] = str(eachRow[1][index])
		# jsonResult = json.dumps(commonParar)
		jsonMap["common"] = commonParar
		# frequence figure
		frequenceFigureMap = {"echartsType": "bar", "echartsUnit": "%", "name": u"频次分布图", "data": []}
		tableArray = []
		for eachRow in freqenceDf.iterrows():
			print(freqenceDf.head())
			tableMap = {}
			tableMap["day"] = str(list(eachRow[1])[0])
			tableMap["frequence"] = str(list(eachRow[1])[1])
			tableArray.append(tableMap)
		frequenceFigureMap['data'] = tableArray

		jsonMap['tabs'].append(frequenceFigureMap)

		finalResult = json.dumps(jsonMap)
		print(finalResult)

		f = open('d://chaoqiJson.txt', 'w')
		f.write(finalResult)
		f.close()
		break

		finalData.to_excel('E:/数联易康/2016-12/德阳项目反馈/超期住院/' + str(i) + 'age' + str(j) + 'lev' + str(h) + '_2015.xlsx')


if __name__ == '__main__':
	soruceData = readdata(path)
	curdata, fredata, i, j, h = dataalyse(soruceData)
	output(curdata, fredata, i, j, h)