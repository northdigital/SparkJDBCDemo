from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession\
  .builder\
  .appName('checkPversion App')\
  .master('spark://centos07:7077')\
  .getOrCreate()

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
df_numbers = spark.sparkContext.parallelize(numbers, 10)

print(df_numbers.getNumPartitions())
print(df_numbers.collect())

input('press <enter> to exit')

spark.sparkContext.stop()
