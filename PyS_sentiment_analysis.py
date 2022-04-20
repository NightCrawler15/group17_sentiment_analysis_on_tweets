import findspark
findspark.init('/opt/spark')

import re
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SQLContext, SparkSession
sql = SQLContext(sc)
# spark = SparkSession(sc)

from textblob import Blobber
from textblob.sentiments import NaiveBayesAnalyzer

class SentimentAnalyzer():
    def __init__(self, file_location):
        self.tweets_sentiments = {"Negative": 0, "Positive": 0, "Neutral": 0}
        self.__load_data(file_location)

    def __load_data(self, file_location):
        tweets_df = sql.read.csv(file_location, header=False, inferSchema= True)
        # tweets_df.show()
        tweets_analyze = self.__analyze_text
        tweets_df.rdd.foreach(lambda x: tweets_analyze(x['_c1']))
        return

    def __analyze_text(self, txt):
        # sentiments = {"Negative": 0, "Positive": 0, "Neutral": 0}
        self.tweets_sentiments[self.__analyzer(txt)] += 1
        return

    def __clean_data(self, txt):
        # Remove mentions
        txt = re.sub(r'@[A-Za-z0-9_]+', '', txt)
        # Remove hashtags
        txt = re.sub(r'#[A-Z0-9]+', '', txt)
        # Remove retweets:
        txt = re.sub(r'RT : ', '', txt)
        # Remove urls
        txt = re.sub(r'https?:\/\/[A-Za-z0-9\.\/]+', '', txt)
        #remove amp
        txt = re.sub(r'&amp;', '', txt)
        #rempve strange characters
        txt = re.sub(r'ðŸ™', '', txt)
        #remove new lines
        txt = re.sub(r'\n', ' ', txt)
        #converting lower text
        txt = txt.lower()
        return txt

    def __analyzer(self, txt):
        # Initializing Native Bayes analyzer
        # Sentiment intensity analyser uses Naiive Bayes 
        blob = Blobber(analyzer=NaiveBayesAnalyzer())
        # passing the filtered data and
        # classifying into positive/ negative using naive bayes
        classifier = blob(self.__clean_data(txt)).sentiment

        if round(classifier[1], 2) > round(classifier[2], 2):
            return "Positive"
        elif round(classifier[1], 2) < round(classifier[2], 2):
            return "Negative"
        else:
            return "Neutral"

if __name__ == '__main__':
    spark_sentiment_analyzer = SentimentAnalyzer("hdfs://namenode:9000/group17_sentiment_analysis_on_tweets/covid_tweets_clean.csv")
    sentiments_df = spark_sentiment_analyzer.tweets_sentiments
    print(type(sentiments_df), sentiments_df)
