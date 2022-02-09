from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

PARQUET_FORMAT = 'parquet'
OVERWRITE_MODE = 'overwrite'
VOICE = 'voice'
PLAYER = 'actor/actress'


# Cast
def get_cast_table(cast_members_arg: DataFrame) -> None:
    cast = cast_members_arg \
        .withColumnRenamed('id', 'cast_member_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'cast_member_id', 'film_id', 'name')

    cast.write.saveAsTable(name='films.DimCast', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Characters
def get_roles_table(cast_members_arg: DataFrame) -> None:
    character = cast_members_arg \
        .withColumnRenamed('id', 'cast_member_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'cast_member_id', 'character')

    character.write.saveAsTable(name='films.DimRoles', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Cast Roles
def get_characters_table(cast_members_arg: DataFrame) -> None:
    role = cast_members_arg \
        .withColumn('is_voice', f.col('character').contains('(voice)')) \
        .withColumn('role', f.when(f.col('is_voice'), VOICE).otherwise(PLAYER)) \
        .withColumnRenamed('id', 'cast_member_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'cast_member_id', 'role').where(f.col('role') == PLAYER)

    role.write.saveAsTable(name='films.DimCharacters', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


app_name = 'final_project'

conf = SparkConf()

hdfs_host = 'hdfs://namenode:8020'
data_lake_path = f'{hdfs_host}/data_lake'

conf.set("hive.metastore.uris", "http://hive-metastore:9083")
conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .config(conf=conf) \
    .getOrCreate()

spark.sql(f"create schema if not exists films location '{hdfs_host}/movies'")

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
    .csv('credits.csv', header=True) \
    .withColumn('cast', f.regexp_replace(f.col('cast'), ': None', ': null')) \
    .withColumn('cast', f.regexp_replace(f.col('cast'), "\\\\'", "")) \
    .withColumn('cast', f.regexp_replace(f.col('cast'), "\\\\", ""))

creds = creds \
    .withColumn('cast_members', f.from_json(creds.cast, cast_schema))

cast_members = creds \
    .withColumn('member', f.explode('cast_members')).withColumnRenamed('id', 'film_id') \
    .select('film_id', 'member.name', 'member.gender', 'member.character', 'member.id')

get_cast_table(cast_members)
get_characters_table(cast_members)
get_roles_table(cast_members)
