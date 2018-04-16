# Import statements
from pyspark.conf import SparkConf
from pyspark.context import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import StorageLevel
from datetime import datetime
import itertools
import sys

reload(sys)
sys.setdefaultencoding('UTF8')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .appName("Fetch Twitter Data")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *

import os
import tweepy

auth = tweepy.OAuthHandler('7ABbdn6aUPHc4X1Mf4TacyCjU', '0695ItebpBXcFvVo6MmKVg4KZ7lzpKWBpJi4s8SLh4V2K087am')
auth.set_access_token('974727938225647616-r7tFYAQWFkS01Ub9XWaEl4WssuYn7g2', 'TBAo16Gqo3FtfOHdxzZwrjnhQt3okJ5YNZDLZEN5l3HDa')
api = tweepy.API(auth)
page_count = 0

tweet_results = []
start_date = "2018-03-14"
end_date = "2018-03-16"
for tweets in tweepy.Cursor(api.search,q='health',count=100,result_type="recent",
                            include_entities=True,since=start_date, until=end_date,
                            geocode='43.672267,-79.390415,100km',
                            lang='en').pages():
    page_count+=1
    
    for tweet in tweets:
        tweet_results.append((str(tweet.id),
                              str(tweet.created_at),
                              tweet.text.encode('utf-8'),
                              str(tweet.source),
                              str(tweet.user.id),
                              tweet.user.name.encode('utf-8'),
                              tweet.user.screen_name.encode('utf-8'),
                              str(tweet.user.location)))
        
    if page_count >= 10:
        break

schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_screen_name", StringType(), True),
    StructField("user_location", StringType(), True),
])

sc = spark.sparkContext
tweet_results_rdd = sc.parallelize(tweet_results)
tweet_results_df = spark.createDataFrame(tweet_results_rdd, schema)
tweet_results_df.write.csv('wasb://tweets@mohdemo.blob.core.windows.net/rawtweets/', header = True, mode = 'overwrite')

