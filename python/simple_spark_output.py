from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .appName("SparkonADF - Simple")\
     .enableHiveSupport()\
     .getOrCreate()

from pyspark.sql.functions import *

## Read in HVAC file(s)
df0 = spark.read.csv('wasb://tweets@mohdemo.blob.core.windows.net/*', header = True, inferSchema = True)

## Get Avg Temp by BuildingID
# df1 = df0.select(col('BuildingID'), col('ActualTemp')).groupBy('BuildingID').avg('ActualTemp')

## Write results to CSV file
blob_address = 'wasb://tweets@mohdemo.blob.core.windows.net/transformresults/'
df0.write.csv('wasb://tweets@mohdemo.blob.core.windows.net/transformresults/', header = True, mode = 'overwrite')
