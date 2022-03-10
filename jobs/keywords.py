from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
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


keywords = spark\
  .read\
  .option("multiLine", "true")\
  .option("quote", '"')\
  .option("header", "true")\
  .option("escape", '"')\
  .option("wholeFile", True)\
  .csv("/airflow/jobs/keywords.csv", header=True)

keywords.printSchema()
keywords_schema = ArrayType(StructType([StructField('id',IntegerType(),True),
                        StructField('name',StringType(),True)]))

keywords = keywords\
    .withColumn('keyword', f.from_json(f.col('keywords'), keywords_schema))\
    .withColumn('keyword', f.explode(f.col("keyword.name"))).withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id()).select('id', 'film_id', 'keyword')

keywords.write.mode('overwrite').parquet(f'{data_lake_path}/keywords.parquet')
spark.read.parquet(f'{data_lake_path}/keywords.parquet').show()

