import findspark
findspark.init('/opt/spark')

import re
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SQLContext, SparkSession
sql = SQLContext(sc)
# spark = SparkSession(sc)

WORDS_DICT = {}

count = 0
tweets_sentiments = {"Negative": 0, "Positive": 0, "Neutral": 0}

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
    if word in WORDS_DICT:
        return WORDS_DICT[word]
    else:
        return 0

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
    sizeTweets = tweets_df.rdd.count()
    tweets_df.rdd.foreach(lambda x: analyze_text(x['_c0'], sizeTweets))
    return

def analyze_text(txt, sizeTweets):
    global tweets_sentiments
    global count
    count += 1
    # print(sentiment)
    text = clean_data(str(txt))
    sentiment = analyzer(text)
    #print(sentiment)
    tweets_sentiments[sentiment] += 1
    if count == sizeTweets:
        print(tweets_sentiments)
    return

# Sentiment analysis takes place here
def analyzer(txt):
    # Tokenizing the words and converting into UTF-8
    # Sentiment intensity analyser uses Naiive Bayes to analyse intensity of the text
    text = txt if txt != '' else "This is a dummy text!"
    pos = 0
    neg = 0
    total = 0
    for word in text.split():
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
        return "Positive"
    elif  round(pos/total, 5) <  round(abs(neg/total), 5):
        return "Negative"
    else:
        return "Neutral"

if __name__ == '__main__':
    WORDS_DICT = words_score_dict()
    load_data("hdfs://namenode:9000/group17_sentiment_analysis_on_tweets/covid_tweets_clean.csv")
    # print(tweets_sentiments)
