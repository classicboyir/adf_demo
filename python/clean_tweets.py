# ======================================================
# In this file, I created 


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
import itertools
import sys

reload(sys)
sys.setdefaultencoding('UTF8')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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
def tweet_cleaner_updated(text):
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

spark = SparkSession.builder \
     .appName("Clean Incoming Tweets")\
     .enableHiveSupport()\
     .getOrCreate()

df0 = spark.read.csv('wasb://tweets@mohdemo.blob.core.windows.net/rawtweets/*', header = True, inferSchema = True)
df0.cache()
df0.show()
print('Total rows of df0: {}'.format(df0.count()))
df = df0.dropna().toPandas()
print('Type of df: {}'.format(type(df)))


clean_tweet_texts = []
for i in xrange(0,len(df)):
    clean_tweet_texts.append(tweet_cleaner_updated(df['tweet'][i]))

df['Cleaned_tweets'] = clean_tweet_texts
schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_screen_name", StringType(), True),
    StructField("user_location", StringType(), True),
    StructField("cleaned_tweets", StringType(), True)
])

# clean_df.to_csv('clean_tweet.csv',encoding='utf-8')
clean_sdf = spark.createDataFrame(df, schema)
clean_sdf.show()
clean_sdf.write.csv('wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/incoming', header = True, mode = 'overwrite')
