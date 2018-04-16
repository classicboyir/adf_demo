# ====================================================
#         ** Developed by Hossein Sarshar **
#
# In this file, the training data is cleaned and 
# stored in a Blob storage
# ====================================================


import pandas as pd  
import numpy as np
import re
from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer
from pyspark.conf import SparkConf
from pyspark.context import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import StorageLevel
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import itertools
import sys

reload(sys)
sys.setdefaultencoding('UTF8')

def tweet_cleaner(text):
    tok = WordPunctTokenizer()

    pat1 = r'@[A-Za-z0-9_]+'
    pat2 = r'https?://[^ ]+'
    combined_pat = r'|'.join((pat1, pat2))
    www_pat = r'www.[^ ]+'
    negations_dic = {"isn't":"is not", "aren't":"are not", "wasn't":"was not", "weren't":"were not",
                    "haven't":"have not","hasn't":"has not","hadn't":"had not","won't":"will not",
                    "wouldn't":"would not", "don't":"do not", "doesn't":"does not","didn't":"did not",
                    "can't":"can not","couldn't":"could not","shouldn't":"should not","mightn't":"might not",
                    "mustn't":"must not"}

    neg_pattern = re.compile(r'\b(' + '|'.join(negations_dic.keys()) + r')\b')

    soup = BeautifulSoup(text, 'lxml')
    souped = soup.get_text()
    try:
        bom_removed = souped.decode("utf-8-sig").replace(u"\ufffd", "?")
    except:
        bom_removed = souped

    stripped = re.sub(combined_pat, '', bom_removed)
    stripped = re.sub(www_pat, '', stripped)
    lower_case = stripped.lower()
    neg_handled = neg_pattern.sub(lambda x: negations_dic[x.group()], lower_case)
    letters_only = re.sub("[^a-zA-Z]", " ", neg_handled)

    # During the letters_only process two lines above, it has created unnecessay white spaces,
    # I will tokenize and join together to remove unneccessary white spaces
    words = [x for x  in tok.tokenize(letters_only) if len(x) > 1]
    return (" ".join(words)).strip()

input_file_address = 'wasb://tweets@mohdemo.blob.core.windows.net/trainingdata/training.csv'

spark = SparkSession.builder \
     .appName("Clean Training Tweets")\
     .enableHiveSupport()\
     .getOrCreate()

schema = StructType([
    StructField("sentiment", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("type", StringType(), True),
    StructField("username", StringType(), True),
    StructField("tweet", StringType(), True)
])

df0 = spark.read.format("csv").option("header", "false").schema(schema).load(input_file_address)
df0.cache()

def sentiment_restruct(sentiment):
    if sentiment == 0:
        return 0
    elif sentiment == 4:
        return 1
    else:
        return sentiment

sentiment_restruct_udf = udf(sentiment_restruct, IntegerType())
tweet_cleaner_updated_udf = udf(tweet_cleaner, StringType())
actualdf = df0.sample(False, 0.5, seed=0)
df_cleaned = actualdf.withColumn('cleaned_tweets', tweet_cleaner_updated_udf(df0['tweet']))
df_cleaned = df_cleaned.withColumn('cleaned_sentiment', sentiment_restruct_udf(df0['sentiment']))
df_cleaned.cache()
df_cleaned.show()

schema = StructType([
    StructField("sentiment", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("type", StringType(), True),
    StructField("username", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("cleaned_tweets", StringType(), True),
    StructField("cleaned_sentiment", IntegerType(), True)
])

out_file_address = 'wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/trainingdata/'
df_cleaned.write.csv(out_file_address, header = True, mode = 'overwrite')
