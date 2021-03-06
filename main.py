from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession\
  .builder\
  .appName('JDBCDemo App')\
  .master('spark://centos07:7077')\
  .getOrCreate()

print('reading logismos.trak_detail table')

df_trak_detail = spark.read.format('jdbc')\
  .option('url', 'jdbc:oracle:thin:@centos06:1521/casinodev')\
  .option('driver', 'oracle.jdbc.OracleDriver')\
  .option('user', 'system')\
  .option('password', 'sporades')\
  .option('dbtable', 'logismos.trak_detail')\
  .load()\
  .cache()

print(f'trak_detail table has {df_trak_detail.count()} records')
# df_trak_detail.printSchema()
# df_trak_detail.show(10)

df_small = df_trak_detail\
  .select('MEMB_LINKID', 'GAMEDATE', 'PLAY_TIME', 'AVG_BET')\
  .cache()
# df_small.show(10)

print('filtering...')

df_small_f = df_small.filter('MEMB_LINKID >= 300760 and MEMB_LINKID <= 300770')
# df_small_f = df_small.filter((df_small['MEMB_LINKID'] >= 300760) & (df_small['MEMB_LINKID'] <= 300770))
# df_small_f = df_small.filter((col('MEMB_LINKID') >= 300760) & (col('MEMB_LINKID') <= 300770))

# df_small_f.show(100)
print(df_small_f.count())

# df_small_f.coalesce(1).write.csv(path='df_small_f3.csv', header=True,mode='overwrite')
df_small_f.write.format('jdbc')\
  .option('url', 'jdbc:oracle:thin:@centos06:1521/casinodev')\
  .option('driver', 'oracle.jdbc.OracleDriver')\
  .option('user', 'system')\
  .option('password', 'sporades')\
  .option('dbtable', 'casinocrm.datawarehouse')\
  .mode('overwrite')\
  .save()

input('press <enter> to exit')

spark.sparkContext.stop()


