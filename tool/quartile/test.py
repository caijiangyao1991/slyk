# coding=utf-8
import pandas as pd
import numpy as np
import quartile

data = [1,2,4,6,8,9,11]
a = quartile.Quartile(data,0.25,0.75)
print a.result




