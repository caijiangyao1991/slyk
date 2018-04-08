# -*- encoding: utf-8 -*-

import MySQLdb
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')





def getid(idCard1, inTime1, idCard2, inTime2):
    """
    :return: 就诊id1, 就诊id2
    """
    con = MySQLdb.connect(host='192.168.2.134', user='root', passwd='123456', db='sync_dy', port=5029, charset='utf8')
    cur = con.cursor()
    patientId = []
    cur.execute("SELECT a3 from yb_yyzyjsd where (a14=%s and a6 = %s) or (a14=%s and a6 = %s)", (idCard1, inTime1, idCard2, inTime2))
    for each in cur:
        patientId.append(each)
    cur.close()
    con.close()
    return patientId


def getDrug(patientId):
    con = MySQLdb.connect(host='192.168.2.134', user='root', passwd='123456', db='sync_dy', port=5029, charset='utf8')
    cur1 = con.cursor()
    cur2 = con.cursor()
    cur1.execute("SELECT distinct(a6) FROM yb_zyyyfmx where a2=%s ", (patientId[0]))
    cur2.execute("SELECT distinct(a6) FROM yb_zyyyfmx where a2=%s ", (patientId[1]))

    drug1 = set()
    drug2 = set()
    for eachdrug1 in cur1:
        drug1.add(eachdrug1)
    for eachdrug2 in cur2:
        drug2.add(eachdrug2)
    cur1.close()
    cur2.close()
    con.close()

    return drug1,drug2


def getProgram(patientId):
    con = MySQLdb.connect(host='192.168.2.134', user='root', passwd='123456', db='sync_dy', port=5029, charset='utf8')
    cur1 = con.cursor()
    cur2 = con.cursor()
    cur1.execute("SELECT distinct(a6) FROM yb_zyylfmx where a2=%s ", (patientId[0]))
    cur2.execute("SELECT distinct(a6) FROM yb_zyylfmx where a2=%s ", (patientId[1]))

    program1 = set()
    program2 = set()
    for eachprogram1 in cur1:
        program1.add(eachprogram1)
    for eachprogram2 in cur2:
        program2.add(eachprogram2)
    cur1.close()
    cur2.close()
    con.close()
    return program1,program2

def similarity(drug1,drug2,program1,program2):
    jiaodrug= drug1 & drug2
    bindrug= drug1 | drug2
    diffdrug=bindrug-jiaodrug
    jiaoprogram= program1 & program2
    binprogram= program1 | program2
    diffprogram = binprogram - jiaoprogram
    similarDrugRatio= float(len(jiaodrug))/float(len(bindrug))
    similarProgramRatio = float(len(jiaoprogram)) / float(len(binprogram))

    print(similarDrugRatio, len(drug1), len(drug2))
    print "相似药品"+ '/b'
    for each in jiaodrug:
        print each[0]

    print "不同药品"'/b'
    for each in diffdrug:
        print each[0]

    print(similarProgramRatio, len(program1), len(program2))

    print "相似检查"'/b'
    for each in jiaoprogram:
        print each[0]

    print "不同检查"'/b'
    for each in diffprogram:
        print each[0]

    return similarDrugRatio,similarProgramRatio




if __name__ == '__main__':
    idCard1 = '510602196302156661'
    idCard2 = '510602195610251164'
    inTime1list= ['2014-01-08']
    inTime2list = [ '2014-01-28']

    for inTime1,inTime2  in zip(inTime1list,inTime2list):
        rawid = getid(idCard1, inTime1, idCard2, inTime2)
        drug1, drug2 = getDrug(rawid)
        program1, program2 = getProgram(rawid)
        drugratio= similarity(drug1,drug2,program1,program2)
