# import csv
import pandas
import csv
import os.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TWEETS = os.path.join(BASE_DIR, "tweets.csv")
tweets = pandas.read_csv(TWEETS, sep = ';', header = None, on_bad_lines='skip',lineterminator='\n')
tweets[0]
'''with open("bitcoin_tweets_old.csv", "w") as result:
        writer = csv.writer(result)
        for r in tweets:
            
            # Use CSV Index to remove a column from CSV
            #r[3] = r['year']
            print(r[0], r[1], r[2], r[3], r[4], r[6], r[7], r[8])
            writer.writerow((r[0], r[1], r[2], r[3], r[4], r[6], r[7], r[8]))'''
'''with open("bitcoin_tweets.csv", "r") as source:
    reader = csv.reader(source)
      
    with open("bitcoin_tweets_old.csv", "w") as result:
        writer = csv.writer(result)
        for r in reader:
            
            # Use CSV Index to remove a column from CSV
            #r[3] = r['year']
            print(r[0])
            writer.writerow((r[0], r[1], r[2], r[3], r[4], r[6], r[7], r[8]))'''
