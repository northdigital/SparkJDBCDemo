from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession\
  .builder\
  .appName('JDBCDemo App')\
  .master('spark://aphrodite:7077')\
  .getOrCreate()

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
print(rdd.collect())

df_trak_detail = spark.read.format('jdbc')\
  .option('url', 'jdbc:oracle:thin:@centos06:1521/casinodev')\
  .option('driver', 'oracle.jdbc.OracleDriver')\
  .option('user', 'system')\
  .option('password', 'sporades')\
  .option('dbtable', 'logismos.trak_detail')\
  .load()

print(df_trak_detail.count())

input('press <enter> to exit')

spark.sparkContext.stop()


