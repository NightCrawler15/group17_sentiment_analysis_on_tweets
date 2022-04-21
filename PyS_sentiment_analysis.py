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

import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

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
    tweets_df.rdd.foreach(lambda x: analyze_text(x['_c1']))
    return

def analyze_text(txt):
    global tweets_sentiments
    sentiment = analyzer(txt)
    #print(sentiment)
    tweets_sentiments[sentiment] += 1
    return

def analyzer(txt):
    # Tokenizing the words and converting into UTF-8
    # Sentiment intensity analyser uses Naiive Bayes to analyse intensity of the text
    score = SentimentIntensityAnalyzer().polarity_scores(txt)  
    if score['neg'] > score['pos']:
        return "Negative"
    elif score['pos'] > score['neg']:
        return "Positive"
    else:
        return "Neutral" 

if __name__ == '__main__':
    load_data("hdfs://namenode:9000/group17_sentiment_analysis_on_tweets/covid_tweets_clean.csv")
    print(tweets_sentiments)
