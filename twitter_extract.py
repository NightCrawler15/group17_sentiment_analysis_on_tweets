import json
import csv
import tweepy
import datetime
from tweepy import OAuthHandler

# authentication keys required to access the tweets
consumer_key = 'A7qvVgeq5w4reI7YE4fTXLpoS'
consumer_secret = 'C0Cc2UEroDjuDgGZm6tO1iThdOTOCD6UXq59DKI0aHO5Yd3V3W'
access_token = '1501541633610944515-SqjjlQS0liX8du52Osl9URT94QTWwt'
access_token_secret = 'fBVNDULOae9OCs46fbxCjzQR0yJ2kbxzBZqSotpRETyUb'

# create OAuthHandler object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# set access token and secret
auth.set_access_token(access_token, access_token_secret)

# create tweepy API object to fetch tweets
api = tweepy.API(auth, wait_on_rate_limit=True, retry_count=10, retry_delay=5, retry_errors=set([503]))

def get_tweets():
    # Open/create a file to append data to
    csvFile = open('covid_tweets.csv', 'a')

    #Use csv writer
    csvWriter = csv.writer(csvFile)
    # csvWriter.writerow(["id","user","fullname","url","timestamp","like","retweets","text"])
    # startDate = datetime.datetime(2020, 1, 1, 0, 0, 0)
    endDate =   datetime.datetime(2022, 3, 16, 0, 0, 0)
    # call twitter api to fetch tweets
    fetched_tweets = tweepy.Cursor(api.search_tweets,q=('covid OR corona OR #CovidVaccine since:2020-01-01 until:2022-03-17'), count=2000000, lang = "en").items()
    for tweet in fetched_tweets:
        # Write a row to the CSV file. I use encode UTF-8
        csvWriter.writerow([tweet.id, tweet.user.screen_name, tweet.user.name, tweet.user.url, tweet.created_at, tweet.favorite_count, tweet.retweet_count, tweet.text])
        

    return fetched_tweets

tweets = get_tweets()
