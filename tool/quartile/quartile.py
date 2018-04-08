# -*- coding:UTF-8 -*-

class Quartile():
    """
    parameters:data,*args
    data:data needed for the computed quantile
    *args:percentile/p of computed quantile
    return list of percentile
    """
    def __init__(self, data, *args):
        self.data = self.__listData(data)
        self.result = self.__computerQuartile(args)

    def __listData(self, data):
        data = list(data)
        data.sort()
        return data

    def __quartilt(self, q):
        Qlen = (len(self.data) + 1) * q
        Q = int(Qlen)
        if abs(Qlen - Q) <= abs((Q + 1) - Qlen):
            weightQ1 = (Q + 1) - Qlen
            weightQ2 = Qlen - Q
        else:
            weightQ1 = (Q + 1) - Qlen
            weightQ2 = Qlen - Q
        if Q == 0:
            lowerq = weightQ2 * self.data[Q]
        elif Q == len(self.data):
            lowerq = weightQ1 * self.data[Q - 1]
        else:
            lowerq = weightQ2 * self.data[Q] + weightQ1 * self.data[Q - 1]
        return lowerq

    def __computerQuartile(self, args):
        result = []
        for q in args:
            result.append(self.__quartilt(q))
        return result

if __name__ == '__main__':
    data = [1,2,3,4,5,6,7,8,9,10,11]
    a=Quartile(data, 0.25, 0.75)
    print a.result