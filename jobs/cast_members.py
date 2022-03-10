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

cast_schema = ArrayType(StructType([StructField('cast_id', IntegerType(), True),
                                    StructField('character', StringType(), True),
                                    StructField('credit_id', StringType(), True),
                                    StructField('gender', IntegerType(), True),
                                    StructField('id', IntegerType(), True),
                                    StructField('name', StringType(), True),
                                    StructField('order', IntegerType(), True),
                                    StructField('profile_path', StringType(), True)]))

# Remove escape chars; Python None to JSON null
creds = spark \
    .read \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv('/airflow/jobs/credits.csv', header=True) \
    .withColumn('cast', f.regexp_replace(f.col('cast'), ': None', ': null')) \
    .withColumn('cast', f.regexp_replace(f.col('cast'), "\\\\'", "")) \
    .withColumn('cast', f.regexp_replace(f.col('cast'), "\\\\", ""))

creds = creds \
    .withColumn('cast_members', f.from_json(creds.cast, cast_schema))

# Records that have schema issues:

rec_with_issues = creds.where(f.col('cast_members').isNull())

cast_members = creds \
    .withColumn('member', f.explode('cast_members')).withColumnRenamed('id', 'film_id') \
    .select('film_id', 'member.name', 'member.gender', 'member.character', 'member.id')

database = 'films_v5'

spark.sql(f"create schema if not exists {database} location '{hdfs_host}/films'")

cast = cast_members \
    .withColumnRenamed('id', 'cast_member_id') \
    .withColumn('id', monotonically_increasing_id()) \
    .select('id', 'cast_member_id', 'film_id', 'name')

cast.write.option('path', f'{data_lake_path}/{database}/Cast') \
    .saveAsTable(name=f'{database}.DimCast', format='parquet')

# Character

characters = cast_members \
    .withColumnRenamed('id', 'cast_member_id') \
    .withColumn('id', monotonically_increasing_id()) \
    .select('id', 'cast_member_id', 'character')

characters.write.option('path', f'{data_lake_path}/{database}/Characters') \
    .saveAsTable(name=f'{database}.DimCharacters', format='parquet')

VOICE = 'voice'
PLAYER = 'actor/actress'

role = cast_members \
    .withColumn('is_voice', f.col('character').contains('(voice)')) \
    .withColumn('role', f.when(f.col('is_voice'), VOICE).otherwise(PLAYER)) \
    .withColumnRenamed('id', 'cast_member_id') \
    .withColumn('id', monotonically_increasing_id()) \
    .select('id', 'cast_member_id', 'role').where(f.col('role') == PLAYER)

role.write.option('path', f'{data_lake_path}/{database}/DimRoles') \
    .saveAsTable(name=f'{database}.DimRoles', format='parquet')

spark.sql('select * from films_v5.dimcast').show(10, truncate=False)
