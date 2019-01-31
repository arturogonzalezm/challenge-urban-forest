import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType, StringType, StructType, StructField, ArrayType, BooleanType

from pyspark_utils import calculate_multipolygon_area, calculate_multipolygon_bounds, convert, calculate_forest_bound, \
    join_condition, calculate_forest_rate

root = os.path.dirname(__file__)
config = SparkConf().setAppName('Urban Forest')
conf = (config.setMaster('local[*]')
        .set('spark.executor.memory', '8G')
        .set('spark.num.executors', '8')
        .set('spark.executor.cores', '4')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '10G'))

sc = SparkContext(conf=conf)
# Set new log level: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
sc.setLogLevel("INFO")
sql_context = SQLContext(sc)

# read raw data
df = sql_context.read.json(
    os.path.join(root, 'melb_inner_2016.json'))
df = df.drop('_corrupt_record').dropna()

df.registerTempTable("raw")
interested_frame = sql_context.sql(
    "SELECT sa2_name16 AS suburb_name , sa2_main16 AS suburb_code, geometry.coordinates AS coordinates FROM raw")

# group and aggregate table by suburb
interested_frame.registerTempTable('interested_table')
gr = sql_context.sql(
    "SELECT suburb_name, FIRST(suburb_code) AS suburb_code, collect_list(coordinates) AS multipolygon "
    "FROM interested_table GROUP BY suburb_name")

# calculate suburb area
user_define_function = func.udf(calculate_multipolygon_area, FloatType())
df1 = gr.withColumn('suburb_area', user_define_function('multipolygon'))

multipolygons_bounds_udf = func.udf(calculate_multipolygon_bounds, ArrayType(ArrayType(FloatType())))

# Dataframe of melbourne sa2 suburb
df1 = df1.withColumn('suburb_bound', multipolygons_bounds_udf('multipolygon'))

# load forest data
schema = StructType([
    StructField('area', FloatType(), nullable=False),
    StructField('polygon', StringType(), nullable=False)
])
df2 = sql_context.read.format("com.databricks.spark.csv").option("header", "false").option('delimiter', ' '). \
    load(os.path.join(root, "melb_urban_forest_2016.txt/part-*"), schema=schema)

# transform geometry string to formatted string
df2 = df2.withColumn('polygon_formatted', func.udf(convert, StringType())('polygon')).drop('polygon').withColumnRenamed(
    'polygon_formatted', 'polygon')

calculate_forest_bound_udf = func.udf(calculate_forest_bound, ArrayType(FloatType()))
df2 = df2.withColumn('forest_bound', calculate_forest_bound_udf('polygon'))

join_condition_udf = func.udf(join_condition, BooleanType())

# join suburb dataframe with forest dataframe based on the condition of intersection
df1p1 = df1.repartition(1)
df2p1 = df2.repartition(10)
joined = df1p1.crossJoin(df2p1).where(join_condition_udf(df1p1.suburb_bound, df2p1.forest_bound))

joined = joined.drop('suburb_bound').drop('area').drop('forest_bound')

# group and aggregate joined table by suburb code(name)
joined.registerTempTable('joined_table')
sql_context.cacheTable("joined_table")
result = sql_context.sql(
    'SELECT FIRST(suburb_name) AS suburb_name, suburb_code, FIRST(multipolygon) AS multipolygon, FIRST(suburb_area) '
    'AS suburb_area, collect_list(polygon) AS forest_multipolygon FROM joined_table GROUP BY suburb_code')

result = result.withColumn('percentage_%', func.udf(calculate_forest_rate, FloatType())(
    'multipolygon', 'forest_multipolygon', 'suburb_area'))
result = result.drop('multipolygon').drop('suburb_area').drop('forest_multipolygon')
result.orderBy('percentage_%', ascending=False).show()
