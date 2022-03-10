from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import monotonically_increasing_id

print(" ---------------- :: STARTED :: ---------------")

app_name = 'final_project'

conf = SparkConf()

hdfs_host = 'hdfs://namenode:8020'

conf.set("hive.metastore.uris", "http://hive-metastore:9083")
conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=conf) \
    .getOrCreate()

data_lake_path = f'{hdfs_host}/data_lake'


ratings = spark\
  .read\
  .option("multiLine", "true")\
  .option("quote", '"')\
  .option("header", "true")\
  .option("escape", '"')\
  .option("wholeFile", True)\
  .csv("/airflow/jobs/ratings.csv", header=True)\


# ratings.printSchema()
# ratings = ratings\
#   .withColumn('rating', f.regexp_replace(f.col("rating"), ': None', '0'))

freq = ratings.groupby(f.col("movieId")).agg(f.count('*').alias("freq"))

freq.show()

avg = ratings.groupby(f.col('movieId')).agg(f.mean(f.col("rating")))\
  .withColumn('id', monotonically_increasing_id())\
  .withColumnRenamed('avg(rating)', 'rating')\
  .select('id', 'movieId', 'rating')

avg.write.mode('overwrite').parquet(f'{data_lake_path}/avg_votes.parquet')
spark.read.parquet(f'{data_lake_path}/avg_votes.parquet').show()
avg.show()

freq.write.mode('overwrite').parquet(f'{data_lake_path}/votes_count.parquet')
spark.read.parquet(f'{data_lake_path}/votes_count.parquet').show()
