#encode=utf8
from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext, SparkConf
import os



os.environ['HADOOP_HOME']='E:\hadoop-2.7.1'
os.environ['SPARK_HOME']='E:\spark-1.6.1-hadoop-2.7.1'

print (os.environ["SPARK_HOME"])
# print (os.environ["SPARK_CLASSPATH"])
APP_NAME = "FPGrowth"
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster('spark://192.168.10.30:7077')

sc = SparkContext(conf=conf)

data = sc.textFile("hdfs:///tmp/itemsetClean.csv")
transactions = data.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.0005, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)
