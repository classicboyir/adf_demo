
# Import statements
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

spark = SparkSession.builder \
     .appName("Clean Training Tweets")\
     .enableHiveSupport()\
     .getOrCreate()

schema = StructType([
    StructField("sentiment", IntegerType()x),
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("type", StringType(), True),
    StructField("username", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("cleaned_tweets", StringType(), True),
    StructField("cleaned_sentiment", IntegerType(), True)
])
df0 = spark.read.format("csv").option("header", "true").schema(schema).load('wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/trainingdata/*')
df0 = df0.dropna()
df0.cache()
df0.show()

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

(train_set, val_set, test_set) = df0.randomSplit([0.98, 0.01, 0.01], seed = 2000)
tokenizer = Tokenizer(inputCol="cleaned_tweets", outputCol="words")
hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol='tf')
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "cleaned_sentiment", outputCol = "label")
pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])

pipelineFit = pipeline.fit(train_set)
train_df = pipelineFit.transform(train_set)
val_df = pipelineFit.transform(val_set)
train_df.show(5)
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=100)
lrModel = lr.fit(train_df)

'''predictions = lrModel.transform(val_df)

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
evaluator.evaluate(predictions)
accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(val_set.count())
print('Accuracy {}'.format(accuracy))

pipeline.write.overwrite.save("sample-pipeline")
'''

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

dfunseen = spark.read.format("csv").option("header", "true").schema(schema).load('wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/incoming')
dfunseen.cache()
print(dfunseen.count())
dfunseen.show()
dfunseen = dfunseen.dropna()
dfunseen = dfunseen.withColumn("cleaned_sentiment", lit(0))

pipelineFit = pipeline.fit(dfunseen)
dfunseen_df = pipelineFit.transform(dfunseen)
predictions = lrModel.transform(dfunseen_df)
final_result = predictions.select('id', 'created_at', 'user_id', 'user_name', 'user_screen_name', 'user_location', 'cleaned_tweets', 'tweet', 'prediction')
final_result.show()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_screen_name", StringType(), True),
    StructField("user_location", StringType(), True),
    StructField("cleaned_tweets", StringType(), True),
    StructField('rawPrediction', StringType(),True),
    StructField('probability', StringType(),True),
    StructField('prediction', DoubleType(),True)
])

final_result.write.csv('wasb://tweets@mohdemo.blob.core.windows.net/prediction_results/', header = True, mode = 'overwrite')



# Edits:
'''from pyspark.ml.classification import LogisticRegressionModel

lrModel.write().overwrite().save("wasb://tweets@mohdemo.blob.core.windows.net/prediction_results/sample-pipeline")
rmodel = LogisticRegressionModel.load("wasb://tweets@mohdemo.blob.core.windows.net/prediction_results/sample-pipeline/")

dfunseen = spark.read.format("csv").option("header", "true").schema(schema).load('wasb://tweets@mohdemo.blob.core.windows.net/cleanedtweets/incoming')
dfunseen.cache()
print(dfunseen.count())
dfunseen.show()
dfunseen = dfunseen.dropna()
dfunseen = dfunseen.withColumn("cleaned_sentiment", lit(0))

pipelineFit = pipeline.fit(dfunseen)
dfunseen_df = pipelineFit.transform(dfunseen)

predictions = rmodel.transform(dfunseen_df)

final_result = predictions.select('id', 'created_at', 'user_id', 'user_name', 'user_screen_name', 'user_location', 'cleaned_tweets', 'tweet', 'prediction')
final_result.show()
'''