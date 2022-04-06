import findspark
findspark.init('/opt/spark')

import pyspark
from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import Row
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df1 = sqlContext.read.csv("hdfs://namenode:9000/group17_sentiment_analysis_on_tweets/covid_tweets.csv", header=True, inferSchema= True)
df1.show()
