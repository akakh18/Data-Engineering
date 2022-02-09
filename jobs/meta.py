from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

PARQUET_FORMAT = 'parquet'
OVERWRITE_MODE = 'overwrite'


def read_data(spark_arg: SparkSession) -> DataFrame:
    result = spark_arg \
        .read \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("header", "true") \
        .option("escape", '"') \
        .option("wholeFile", True) \
        .csv("movies_metadata.csv", header=True)

    result = result \
        .where("imdb_id not in ('0', 'tt0113002', 'tt2423504', 'tt2622826')")

    prod_countries = 'production_countries'
    result = result \
        .withColumn(prod_countries, f.regexp_replace(f.col(prod_countries), ': None', ': null')) \
        .withColumn(prod_countries, f.regexp_replace(f.col(prod_countries), "\\\\'", " ")) \
        .withColumn(prod_countries, f.regexp_replace(f.col(prod_countries), "\\\\", " "))

    prod_countries_schema = spark.read.json(result.rdd.map(lambda row: row.production_countries)).schema

    # discard '_corrupt_record'
    # prod_countries_schema = StructType(list(prod_countries_schema)[1:])

    prod_countries_schema = ArrayType(prod_countries_schema)

    result = result \
        .withColumn('prod_countries', f.from_json(f.col(prod_countries), prod_countries_schema))

    return result


# Films
def get_films_table(meta_arg: DataFrame) -> None:
    films = meta_arg.select('id', 'title', 'original_title')
    films.write.saveAsTable(name='films.FactFilms', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Countries
def get_countries_table(meta_arg: DataFrame) -> None:
    prod_countries = meta_arg \
        .withColumn('country', f.explode(f.col('prod_countries'))) \
        .withColumn('country_id', monotonically_increasing_id()) \
        .withColumnRenamed('id', 'film_id') \
        .select('country_id', 'film_id', 'country.name')

    prod_countries.write.saveAsTable(name='films.DimCountries', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Genres
def get_genres_table(meta_arg: DataFrame) -> None:
    genres_schema = ArrayType(StructType([StructField('id', IntegerType(), True),
                                          StructField('name', StringType(), True)]))

    genres = meta_arg \
        .withColumn('genres_schema', f.from_json('genres', genres_schema)) \
        .withColumn('genres_explode', f.explode(f.col('genres_schema'))) \
        .withColumnRenamed('id', 'film_id') \
        .select('genres_explode.id', 'film_id', 'genres_explode.name')

    genres.write.saveAsTable(name='films.DimGenres', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Homepages
def get_homepages_table(meta_arg: DataFrame) -> None:
    homepages = meta_arg \
        .withColumn('homepage_id', monotonically_increasing_id()) \
        .withColumnRenamed('id', 'film_id') \
        .select('homepage_id', 'film_id', 'homepage')

    homepages.write.saveAsTable(name='films.DimHomepages', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# imdb_id
def get_imdb_id_table(meta_arg: DataFrame) -> None:
    imdb_id = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'imdb_id')

    imdb_id.write.saveAsTable(name='films.DimImdbId', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Overview
def get_overview_table(meta_arg: DataFrame) -> None:
    overview = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'overview').limit(10)

    overview.write.saveAsTable(name='films.DimOverviews', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Popularity
def get_popularity_table(meta_arg: DataFrame) -> None:
    popularity = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'popularity')

    popularity.write.saveAsTable(name='films.DimPopularity', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Production Companies
def get_companies_table(meta_arg: DataFrame) -> None:
    companies_schema = ArrayType(StructType([StructField('id', IntegerType(), True),
                                             StructField('name', StringType(), True)]))

    production_companies = meta_arg \
        .withColumn('company', f.from_json(f.col('production_companies'), companies_schema)) \
        .withColumn("company_name", f.explode('company.name')) \
        .withColumn('company_id', monotonically_increasing_id()) \
        .withColumnRenamed('id', 'film_id') \
        .select('company_id', 'film_id', 'company_name')

    production_companies.write.saveAsTable(name='films.DimProductionCompanies', format=PARQUET_FORMAT,
                                           mode=OVERWRITE_MODE)


# Release Date
def get_date_table(meta_arg: DataFrame) -> None:
    release_date = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'release_date')

    release_date.write.saveAsTable(name='films.DimReleaseDates', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Revenue
def get_revenue_table(meta_arg: DataFrame) -> None:
    revenue = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'revenue', 'imdb_id')

    revenue.write.saveAsTable(name='films.DimRevenues', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Runtime
def get_runtime_table(meta_arg: DataFrame) -> None:
    runtime = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'runtime')

    runtime.show()
    runtime.write.saveAsTable(name='films.DimRuntimes', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Spoken Languages
def get_languages_table(meta_arg: DataFrame) -> None:
    languages_schema = ArrayType(StructType([StructField('iso_639_1', StringType(), True),
                                             StructField('name', StringType(), True)]))
    spoken_languages = meta_arg \
        .withColumn('languages_explode', f.from_json(meta.spoken_languages, languages_schema)) \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .withColumn('languages', f.explode(f.col('languages_explode.name'))) \
        .select('id', 'film_id', 'languages')

    spoken_languages.write.saveAsTable(name='films.DimSpokenLanguages', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Status
def get_status_table(meta_arg: DataFrame) -> None:
    status = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'status')

    status.write.saveAsTable(name='films.DimStatus', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Vote Average
def get_vote_average_table(meta_arg: DataFrame) -> None:
    vote_average = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'vote_average')

    vote_average.write.saveAsTable(name='films.DimVoteAverage', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


# Vote Count
def get_vote_count_table(meta_arg: DataFrame) -> None:
    vote_count = meta_arg \
        .withColumnRenamed('id', 'film_id') \
        .withColumn('id', monotonically_increasing_id()) \
        .select('id', 'film_id', 'vote_count')

    vote_count.write.saveAsTable(name='films.DimVoteCount', format=PARQUET_FORMAT, mode=OVERWRITE_MODE)


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

meta = read_data(spark)
get_films_table(meta)
get_countries_table(meta)
get_genres_table(meta)
get_homepages_table(meta)
get_imdb_id_table(meta)
get_overview_table(meta)
get_popularity_table(meta)
get_companies_table(meta)
get_date_table(meta)
get_revenue_table(meta)
get_languages_table(meta)
get_status_table(meta)
get_vote_average_table(meta)
get_vote_count_table(meta)
