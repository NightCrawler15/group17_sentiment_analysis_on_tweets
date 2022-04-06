import csv
import re
import pandas as pd
import os.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TWEETS = os.path.join(BASE_DIR, "tweets.csv")
# tweets = pandas.read_csv(TWEETS, sep = ';', header = None, on_bad_lines='skip',lineterminator='\n')
# tweets[0]
count = 0

'''df = pd.read_csv('train.csv', encoding = "ISO-8859-1")
with open("train_clean.csv", "w") as result:
    writer = csv.writer(result)
    for index, r in df.iterrows():
        print(r)
    # Use CSV Index to remove a column from CSV
    #r[3] = r['year']
    #print(r[0], r[1], r[2], r[3], r[4], r[6], r[7], r[8])
        polarity = "negative" if r[0] == "0" else "positive" if r[0] == "4" else "neutral"
        writer.writerow((r[5], polarity))'''
with open("covid_tweets.csv", "r") as source:
    reader = csv.reader(source)
    count = 0
      
    with open("covid_tweets_clean.csv", "w") as result:
        writer = csv.writer(result)
        for r in reader:
            
            # Use CSV Index to remove a column from CSV
            #r[3] = r['url']
            tweet_id = r[0]
            text = re.sub(r'\n', ' ', r[6])
            writer.writerow((tweet_id, text))
            if (count > 5): break
            count += 1
