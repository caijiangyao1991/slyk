# coding=utf8
import sys
import json
import os
import MySQLdb
reload(sys)
sys.setdefaultencoding('utf8')




def init_changeList(path="./data/changelist.txt"):
	isExist = os.path.exists(path)
	changeParaMap={}
	if isExist:
		f=open(path)
		changeParaMap = json.load(f)
	return changeParaMap

def persist_changeList(changeParaMap,path="./data/changelist.txt"):
	f = open(path,'w')
	json.dump(changeParaMap,f)
	f.close()


#get josn data
def get_resource():
	con = MySQLdb.connect(host='192.168.2.79', user='root', passwd='123456', db='jgsh_pzh', port=3306, charset='utf8')
	cur = con.cursor()
	cur.execute("select distinct(checkTypeName) from su_illegal ")
	descMap = init_desc_map()
	changeMap = init_changeList()
	for eachCheckTypeName in cur:
		print eachCheckTypeName[0]
		typeName=eachCheckTypeName[0]
		typeFile="./data/json_exmaple_" + typeName + ".txt"
		isExist = os.path.exists(typeFile)
		if  isExist:
			f=open(typeFile)
			jsonMap=json.load(f)
			f.close()
		else:
			cur.execute("select descJsonStr from su_illegal where checkTypeName=%s  limit 1",(eachCheckTypeName[0],))
			curRes = cur.fetchall()[0]
			jsonStr=curRes[0]
			jsonMap = processJsonMap(jsonStr)
		paraDescs = gen_json_desc(jsonMap, descMap,changeMap)
		persist_desc_map(descMap)
		save_algorithm_desc(jsonMap, paraDescs,typeName)
		persist_changeList(changeMap)
	cur.close()
	con.close()



def processJsonMap(jsonStr):
	processJsonMap=json.loads(jsonStr,encoding='utf8')
	return processJsonMap

def gen_json_desc(jsonMap,descMap,changeMap):
	commonParas=jsonMap['common']
	paraDescs=[]
	for  eachParas in commonParas.keys():

		curParas = para_input(eachParas,changeMap)
		curValue = commonParas[eachParas]
		if curParas != eachParas:
			commonParas.pop(eachParas)
			commonParas[curParas]=curValue
		curDesc=json_desc(curParas,descMap)

		line = curParas+u":"+curDesc+u"\n"
		paraDescs.append(line)
	return paraDescs

def init_desc_map(filePath ='./data/descMap.txt'):
	isExist=os.path.exists(filePath)
	descMap={}
	if  isExist:
		desdFile=open(filePath)
		descMap=json.load(desdFile)
	return descMap

def persist_desc_map(descMap,filePath ='./data/descMap.txt'):

	f=open(filePath,'w')
	json.dump(descMap,f)
	f.close()

def json_desc(curPara,descMap):
	retParaDesc=""
	if curPara in descMap.keys():

		raw_input("the para "+  descMap[curPara] + " has been described.")
		print descMap[curPara]
		retParaDesc=descMap[curPara]
	else:
		curDesc =raw_input("please input the desc for para "+ curPara)
		descMap[curPara]=curDesc
		retParaDesc=curDesc
	return retParaDesc




def para_input(inputPara,changeMap):
	print inputPara
	newPara=inputPara
	if inputPara in changeMap.keys():
		raw_input("whether to change with "+changeMap[inputPara] +"\n")
		newPara = changeMap[inputPara]
	else:
		strFlag=raw_input("whether to change(y for yes):\n")
		if strFlag=='y':
			newPara =raw_input("please put new word to inplace of the origin:\n")
		changeMap[inputPara]=newPara

	return newPara

def save_algorithm_desc(jsonMap,paraDescs,filename):
	pathExmaple = "./data/json_exmaple_" +filename+".txt"

	f=open(pathExmaple,'w')
	f.write(json.dumps(jsonMap,indent=1))
	f.close()
	pathDesc="./data/json_desc_" +filename+".txt"
	f = open(pathDesc, 'w')
	for eachLine in paraDescs:
		f.write(eachLine)
	f.close()


if __name__=="__main__":
	get_resource()




