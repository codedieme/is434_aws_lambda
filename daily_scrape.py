# import modules
import datetime as dt
import boto3
import json
import pandas as pd
import tweepy
import os
import io
from deep_translator import GoogleTranslator

translator = GoogleTranslator(source="auto", target="en")

today = dt.datetime.now()

yest = today - dt.timedelta(days=1)
yestDay = str(yest.day)
yestMth = str(yest.month)
yestYr = str(yest.year)


if (len(yestDay) == 1):
    yestDay = "0" + yestDay

if (len(yestMth) == 1):
    yestMth = "0" + yestMth


def lambda_handler(event, context):
    # TODO implement

    words = '#turkeyearthquake OR #syriaearthquake OR #turkeysyriaearthquake OR #syriaturkeyearthquake'
    numtweet = 1000
    date_since = yestYr + "-" + yestMth + "-" + yestDay

    # Enter your own credentials obtained
    # from your developer account
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SCRT')
    access_key = os.getenv('ACCESS_KEY')
    access_secret = os.getenv('ACCESS_SCRT')
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # Creating DataFrame using pandas
    db = pd.DataFrame(columns=['username',
                               'description',
                               'location',
                               'following',
                               'followers',
                               'totaltweets',
                               'retweetcount',
                               'text',
                               'hashtags'])

    # We are using .Cursor() to search
    # through twitter for the required tweets.
    # The number of tweets can be
    # restricted using .items(number of tweets)
    tweets = tweepy.Cursor(api.search_tweets, words,
                           since_id=date_since, tweet_mode='extended').items(numtweet)

    # .Cursor() returns an iterable object. Each item in
    # the iterator has various attributes
    # that you can access to
    # get information about each tweet
    list_tweets = [tweet for tweet in tweets]


    # we will iterate over each tweet in the
    # list for extracting information about each tweet
    for tweet in list_tweets:
        username = tweet.user.screen_name
        description = tweet.user.description
        location = tweet.user.location
        following = tweet.user.friends_count
        followers = tweet.user.followers_count
        totaltweets = tweet.user.statuses_count
        retweetcount = tweet.retweet_count
        hashtags = tweet.entities['hashtags']

        # Retweets can be distinguished by
        # a retweeted_status attribute,
        # in case it is an invalid reference,
        # except block will be executed
        try:
            text = translator.translate(tweet.retweeted_status.full_text)
        except AttributeError:
            text = translator.translate(tweet.full_text)
        hashtext = list()
        for j in range(0, len(hashtags)):
            hashtext.append(hashtags[j]['text'])

        # Here we are appending all the
        # extracted information in the DataFrame
        ith_tweet = [username, description,
                     location, following,
                     followers, totaltweets,
                     retweetcount, text, hashtext]
        db.loc[len(db)] = ith_tweet
        
    AWS_S3_BUCKET = os.getenv("BUCKET_NAME")
    AWS_ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("ACCESS_KEY_SECRET")
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    

    with io.StringIO() as csv_buffer:
        db.to_csv(csv_buffer, index=False)
        csv_name = 'turkeysyria_scrape_' + yestMth + "-" + yestDay + ".csv"

        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key=csv_name, Body=csv_buffer.getvalue()
        )
    
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
        if status == 200:
            result = {}
            result['Msg'] = "Scrape & Translation for " + dt.datetime.now().strftime("%m-%d") + " is successful!"
            result['Timestamp'] = dt.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            return result
        else:
            result['Msg'] = "Scrape for " + dt.datetime.now().strftime("%m-%d") + " is unsuccessful! Please check AWS Logs"
            result['Timestamp'] = dt.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            return result

    

