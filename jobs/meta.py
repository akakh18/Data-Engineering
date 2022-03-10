from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import monotonically_increasing_id

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

database = 'films_v5'

spark.sql(f"create schema if not exists {database} location '{hdfs_host}/films'")

meta = spark\
  .read\
  .option("multiLine", "true")\
  .option("quote", '"')\
  .option("header", "true")\
  .option("escape", '"')\
  .option("wholeFile", True)\
  .csv("/airflow/jobs/movies_metadata.csv", header=True)\


meta = meta\
  .where("imdb_id not in ('0', 'tt0113002', 'tt2423504', 'tt2622826')")


prod_countries = 'production_countries'
meta = meta\
  .withColumn(prod_countries, f.regexp_replace(f.col(prod_countries), ': None', ': null')) \
  .withColumn(prod_countries, f.regexp_replace(f.col(prod_countries), "\\\\'", " "))\
  .withColumn(prod_countries, f.regexp_replace(f.col(prod_countries), "\\\\", " "))

meta.printSchema()


prod_countries_schema = spark.read.json(meta.rdd.map(lambda row: row.production_countries)).schema

# discard '_corrupt_record'
# prod_countries_schema = StructType(list(prod_countries_schema)[1:])

prod_countries_schema = ArrayType(prod_countries_schema)

prod_countries_schema


meta = meta\
  .withColumn('prod_countries', f.from_json(f.col(prod_countries), prod_countries_schema))



# Films
films = meta.select('id', 'title', 'original_title')


films.write.option('path', f'{data_lake_path}/{database}/FactFilms')\
    .saveAsTable(name=f'{database}.FactFilms', format='parquet')


# Countries
prod_countries = meta\
    .withColumn('country', f.explode(f.col('prod_countries')))\
    .withColumn('country_id', monotonically_increasing_id())\
    .withColumnRenamed('id', 'film_id')\
    .select('country_id', 'film_id', 'country.name')


prod_countries.write.option('path', f'{data_lake_path}/{database}/DimCountries')\
    .saveAsTable(name=f'{database}.DimCountries', format='parquet')



# Genres
genres_schema = ArrayType(StructType([StructField('id',IntegerType(),True),
                        StructField('name',StringType(),True)]))

genres = meta\
    .withColumn('genres_schema', f.from_json('genres', genres_schema))\
    .withColumn('genres_explode', f.explode(f.col('genres_schema')))\
    .withColumnRenamed('id', 'film_id')\
    .select('genres_explode.id', 'film_id', 'genres_explode.name')


genres.write.option('path', f'{data_lake_path}/{database}/DimGenres')\
    .saveAsTable(name=f'{database}.DimGenres', format='parquet')








# Homepages
homepages = meta\
    .withColumn('homepage_id', monotonically_increasing_id())\
    .withColumnRenamed('id', 'film_id')\
    .select('homepage_id', 'film_id', 'homepage')


homepages.write.option('path', f'{data_lake_path}/{database}/DimHomepages')\
    .saveAsTable(name=f'{database}.DimHomepages', format='parquet')

spark.sql(f'select * from {database}.DimHomepages').show(10, truncate=False)



# imdb_id
imdb_id = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'imdb_id')


imdb_id.write.option('path', f'{data_lake_path}/{database}/DimImdbId')\
    .saveAsTable(name=f'{database}.DimImdbId', format='parquet')

spark.sql(f'select * from {database}.DimImdbId').show(10, truncate=False)

imdb_id.write.mode('overwrite').parquet(f'{data_lake_path}/imdb_id.parquet')

spark.read.parquet(f'{data_lake_path}/imdb_id.parquet').show()


# Overview
overview = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'overview').limit(10)


overview.write.option('path', f'{data_lake_path}/{database}/DimOverview')\
    .saveAsTable(name=f'{database}.DimOverview', format='parquet')

spark.sql(f'select * from {database}.DimOverview').show(10, truncate=False)



# Popularity
popularity = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'popularity')


popularity.write.option('path', f'{data_lake_path}/{database}/DimPopularity')\
    .saveAsTable(name=f'{database}.DimPopularity', format='parquet')

spark.sql(f'select * from {database}.DimPopularity').show(10, truncate=False)




# Production Companies
companies_schema = ArrayType(StructType([StructField('id',IntegerType(),True),
                        StructField('name',StringType(),True)]))

production_companies = prod_countries = meta\
    .withColumn('company', f.from_json(f.col('production_companies'), companies_schema))\
    .withColumn("company_name", f.explode('company.name'))\
    .withColumn('company_id', monotonically_increasing_id())\
    .withColumnRenamed('id', 'film_id')\
    .select('company_id', 'film_id', 'company_name')


prod_countries.write.option('path', f'{data_lake_path}/{database}/DimProductionCountries')\
    .saveAsTable(name=f'{database}.DimProductionCountries', format='parquet')

spark.sql(f'select * from {database}.DimProductionCountries').show(10, truncate=False)





# Release Date
release_date = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'release_date')

release_date.write.option('path', f'{data_lake_path}/{database}/DimReleaseDate')\
    .saveAsTable(name=f'{database}.DimReleaseDate', format='parquet')

spark.sql(f'select * from {database}.DimReleaseDate').show(10, truncate=False)




# Revenue
revenue = release_date = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'revenue')


revenue.write.option('path', f'{data_lake_path}/{database}/DimRevenue')\
    .saveAsTable(name=f'{database}.DimRevenue', format='parquet')

spark.sql(f'select * from {database}.DimRevenue').show(10, truncate=False)






# Runtime
runtime = release_date = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'runtime')


runtime.write.option('path', f'{data_lake_path}/{database}/DimRuntime')\
    .saveAsTable(name=f'{database}.DimRuntime', format='parquet')

spark.sql(f'select * from {database}.DimRuntime').show(10, truncate=False)







# Spoken Languages
languages_schema =  ArrayType(StructType([StructField('iso_639_1', StringType(), True),
                        StructField('name', StringType(), True)]))

# meta.select(meta.spoken_languages).limit(10).write.csv('../data/csv/output/out1')

spoken_languages = meta\
    .withColumn('languages_explode', f.from_json(meta.spoken_languages, languages_schema))\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .withColumn('languages', f.explode(f.col('languages_explode.name')))\
    .select('id', 'film_id', 'languages')


spoken_languages.write.option('path', f'{data_lake_path}/{database}/DimSpokenLanguages')\
    .saveAsTable(name=f'{database}.DimSpokenLanguages', format='parquet')


spark.sql(f'select * from {database}.DimSpokenLanguages').show(10, truncate=False)





# Status
status = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'status')


status.write.option('path', f'{data_lake_path}/{database}/DimStatus')\
    .saveAsTable(name=f'{database}.DimStatus', format='parquet')

spark.sql(f'select * from {database}.DimStatus').show(10, truncate=False)












# Vote Average

vote_average = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'vote_average')


vote_average.write.option('path', f'{data_lake_path}/{database}/DimVoteAverage')\
    .saveAsTable(name=f'{database}.DimVoteAverage', format='parquet')

spark.sql(f'select * from {database}.DimVoteAverage').show(10, truncate=False)







# Vote Count
vote_count = meta\
    .withColumnRenamed('id', 'film_id')\
    .withColumn('id', monotonically_increasing_id())\
    .select('id', 'film_id', 'vote_count').limit(20)


vote_count.write.option('path', f'{data_lake_path}/{database}/DimVoteCount')\
    .saveAsTable(name=f'{database}.DimVoteCount', format='parquet')

spark.sql(f'select * from {database}.DimVoteCount').show(10, truncate=False)





