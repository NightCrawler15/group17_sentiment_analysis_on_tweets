import csv
import re
import pandas as pd
import os.path
count = 0
date = ''
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

df = pd.read_csv('covid_tweets.csv', encoding = "ISO-8859-1")      
with open("covid_tweets_clean.csv", "w") as result:
    writer = csv.writer(result)
    # for r in reader:
    for index, r in df.iterrows():    
        # Use CSV Index to remove a column from CSV
        date = r[3]
        count = count + 1
        tweet_id = r[0]
        text = re.sub(r'\n', ' ', r[6])
        writer.writerow((tweet_id, text))
        if count > 5 : 
            break

print(count, date)
