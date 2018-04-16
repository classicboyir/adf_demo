# Import statements
from pyspark.conf import SparkConf
from pyspark.context import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from datetime import datetime
import itertools
import sys

reload(sys)
sys.setdefaultencoding('UTF8')

conf = SparkConf()
conf.setMaster('local').setAppName("Fetch Tweeter Feeds")
sc = SparkContext.getOrCreate(conf)
sqlContext = SQLContext(sc)

schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("source", StringType(), True),
])

tweet_results = []
tweets = ('id', 'created_at', 'tweet', 'source')
tweet_results.append(tweets)

tweet_results_rdd = sc.parallelize(tweet_results)
tweet_results_df = sqlContext.createDataFrame(tweet_results_rdd, schema)
tweet_results_df.show()


# tweet_results_df.write.parquet("wasb://tweets@mohdemo.blob.core.windows.net/{}/raw_tweets.parquet".format(start_date))
blob_root = '' # 'wasb://tweets@mohdemo.blob.core.windows.net'
blob_address = '{}/simple_result/'.format(blob_root)
tweet_results_df.write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").save(blob_address)
