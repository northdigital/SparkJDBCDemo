from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession\
  .builder\
  .appName('JDBCDemo App')\
  .master('spark://aphrodite:7077')\
  .getOrCreate()

print('reading logismos.trak_detail table')

df_trak_detail = spark.read.format('jdbc')\
  .option('url', 'jdbc:oracle:thin:@centos06:1521/casinodev')\
  .option('driver', 'oracle.jdbc.OracleDriver')\
  .option('user', 'system')\
  .option('password', 'sporades')\
  .option('dbtable', 'logismos.trak_detail')\
  .load()\
  .detail.cache()

print(f'trak_detail table has {df_trak_detail.count()} records')

df_trak_detail.show(10)

small_df = df_trak_detail.select(['MEMB_LINKID', 'GAMEDATE', 'PLAY_TIME', 'AVG_BET'])
small_df.show(10)

spark.sparkContext.stop()


