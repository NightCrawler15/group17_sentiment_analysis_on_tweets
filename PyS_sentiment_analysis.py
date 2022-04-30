import findspark
findspark.init('/opt/spark')

import re
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SQLContext
from pyspark.sql import functions as func

sql = SQLContext(sc)
# spark = SparkSession(sc)

WORDS_DICT = {}

def words_score_dict():
    #Loading the dictionary at the time of initiation
    words_file = open('/home/ubuntu/AFINN-en-165.txt')

    words_dict = {}

    for line in words_file:
        word, score = line.split('\t')
        words_dict[word] = int(score)

    words_file.close()
    return words_dict

def eval_word(word):
    negators = {'not','no','never'}
    sentiment_score = 0
    if word in WORDS_DICT:
        if word in negators:
            sentiment_score = WORDS_DICT.get(word,0) * -1
        else:
            sentiment_score = WORDS_DICT.get(word,0)
    return sentiment_score

def clean_word(word):
    #Extracting only text
    results = re.findall('^#?[a-zA-Z_]*', word)

    if len(results) > 0:
        word = results[0].lower()

    return word

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
    tweets_sentiment = tweets_df.map(lambda x: analyzer(x['_c0']))
    return tweets_sentiment

# Sentiment analysis takes place here
def analyzer(txt):
    # Tokenizing the words and converting into UTF-8
    # Sentiment intensity analyser uses Naiive Bayes to analyse intensity of the text
    # Doing some initial Fltering
    txt = clean_data(str(txt))
    pos = 0
    neg = 0
    total = 0
    for word in txt.split():
        clean_wrd = clean_word(word)
        score = eval_word(clean_wrd)
        total += 1
        if score >= 0:
            pos = pos + score
        else:
            neg = neg + score
    # if empty text
    if total == 0: 
        total = 1
    if round(pos/total, 5) > round(abs(neg/total), 5):
        return ("Positive", 1)
    elif  round(pos/total, 5) <  round(abs(neg/total), 5):
        return ("Negative", 1)
    else:
        return ("Neutral", 1)

if __name__ == '__main__':
    WORDS_DICT = words_score_dict()
    tweets_sentiments = load_data("hdfs://namenode:9000/group17_sentiment_analysis_on_tweets/covid_tweets_clean.csv")
    # Shows total sum of sentiments
    tweets_sentiments.group_by("_c0").agg(func.col("_c0"), func.sum("_c1")).show()
