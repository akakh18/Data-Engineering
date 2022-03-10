
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys
from imdb import IMDb





i = 0
num_processes = 0

if len(sys.argv) != 0:
    i = int(sys.argv[1])
    num_processes = int(sys.argv[2])
else:
    sys.exit(-1)

app_name = 'final_project'

conf = SparkConf()

hdfs_host = 'hdfs://namenode:8020'

conf.set("hive.metastore.uris", "http://hive-metastore:9083")
conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

spark = SparkSession\
.builder\
.appName(app_name)\
.config(conf=conf)\
.getOrCreate()

data_lake_path = f'{hdfs_host}/data_lake'

def save_image(film_id: str) -> str:
    ia = IMDb()
    return ia.get_movie(film_id)['cover url']


staging = spark.read.parquet(f'{data_lake_path}/imdb_id.parquet').limit(5)

size = staging.count()/num_processes

staging = staging.where(f.col("id") >= i * size).where(f.col("id") < (i+1) * size)


save_corresponding_image = udf(
    lambda val: save_image(val[2:]),
    StringType()
)

urls = staging.withColumn('url', save_corresponding_image(staging.imdb_id))
urls.show()
urls.write.mode('overwrite').parquet(f'/airflow/jobs/urls{i}.parquet')
spark.read.parquet(f'/airflow/jobs/urls{i}.parquet').show()
