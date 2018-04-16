import pandas as pd  
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
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
     .appName("SparkonADF - Simple")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *
cleaned_df = spark.read.csv('wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/*', header = True, inferSchema = True)
cleaned_df.show()
cleaned_pdf = cleaned_df.toPandas()

from wordcloud import WordCloud

neg_string = pd.Series(cleaned_pdf['Cleaned_tweets']).str.cat(sep=' ')
print(neg_string)

wordcloud = WordCloud(width=1600, height=800,max_font_size=200).generate(neg_string)
plt.figure(figsize=(12,10))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.savefig('wordcount.png')

# clean_df.to_csv('clean_tweet.csv',encoding='utf-8')
# clean_sdf = spark.createDataFrame(df, schema)
# clean_sdf.show()
# clean_sdf.write.csv('wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/', header = True, mode = 'overwrite')
