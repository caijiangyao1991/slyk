# -*- coding:utf8*-

import pandas as pd
import numpy as np
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder

data = pd.read_excel('test.xlsx', encoding='gb18030', sep="xovm02")
data['disease'] = LabelEncoder().fit_transform(data['disease'])
data['hospital'] = LabelEncoder().fit_transform(data['hospital'])
x = data[['disease','money','hospital']]
y = data['label']
info = mutual_info_classif(x, y, discrete_features=[True, False, True], copy=True, random_state=None)
map = {}
name = ['disease','money','hospital']

for na ,info in zip(name, info):
    map[na]=info

for key, value in map.iteritems():
    if value<0.001:
        print(value)
        print(key)

