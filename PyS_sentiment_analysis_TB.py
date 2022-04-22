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

count = 0
tweets_sentiments = {"Negative": 0, "Positive": 0, "Neutral": 0}

def clean_data(txt):
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

def load_data(file_location):
    tweets_df = sql.read.csv(file_location, header=False, inferSchema= True)
    # tweets_df.show()
    sizeTweets = tweets_df.rdd.count()
    tweets_df.rdd.foreach(lambda x: analyze_text(x['_c0'], sizeTweets))
    return

def analyze_text(txt, sizeTweets):
    # sentiments = {"Negative": 0, "Positive": 0, "Neutral": 0}
    global tweets_sentiments
    global count
    sentiment = analyzer(txt)
    count += 1
    # print(sentiment)
    tweets_sentiments[sentiment] = tweets_sentiments[sentiment] + 1
    if count == sizeTweets:
        print(tweets_sentiments)
    return 

def analyzer(txt):
    # passing the filtered data and
    # Initializing Native Bayes analyzer
    # Sentiment intensity analyser uses Naiive Bayes 
    blob = Blobber(analyzer=NaiveBayesAnalyzer())
    # Doing some initial Fltering
    text = clean_data(str(txt))
    # classifying into positive/ negative using naive bayes
    classifier = blob(text).sentiment

    if round(classifier[1], 2) > round(classifier[2], 2):
        return "Positive"
    elif round(classifier[1], 2) < round(classifier[2], 2):
        return "Negative"
    else:
        return "Neutral"

if __name__ == '__main__':
    load_data("hdfs://namenode:9000/group17_sentiment_analysis_on_tweets/covid_tweets_clean.csv")
    # print(tweets_sentiments)
