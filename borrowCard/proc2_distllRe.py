__author__ = 'Administrator'
f = open("./data/reslt.txt")

lines = f.readlines()

w = open("./data/reslt2.txt",'w')
curLineNum = len(lines)
resultSet = set()
for index in range(0,len(lines)):
    curLine = lines[curLineNum-index-1]
    parts=curLine.split(",")
    print parts[0]
    onePart=(parts[0],parts[1])
    if parts[0]==parts[1] or int(parts[2])<4: continue
    if onePart not in resultSet:
        w.write(curLine)
        resultSet.add(onePart)
    curLineNum-=1
f.close()
w.close()