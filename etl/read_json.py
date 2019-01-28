import re

import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType, StringType, StructType, StructField, MapType, ArrayType, Row, DataType, \
    BooleanType
from shapely import wkt
from shapely.geometry import shape, Polygon, mapping

from polygon_utils import multi_polygon_area, merge_multi_polygons, may_intersect, intersection_area, \
    may_intersect_modified


# conf = (pyspark.SparkConf().setAppName('Urban Forest').setMaster("local[2]"))


def calculate_multipolygon_area(multipolygons):
    # areas = []
    # for multipolygon in multipolygons:
    #     areas.append(multi_polygon_area([multipolygon]))
    # return sum(areas)

    merged = merge_multi_polygons(*multipolygons)
    return multi_polygon_area(merged)


sc = pyspark.SparkContext.getOrCreate()

sql_context = SQLContext(sc)
# read raw data
df = sql_context.read.json(
    '/Users/arturogonzalez/PycharmProjects/challenge-urban-forest/melb_inner_2016.json')
df = df.drop('_corrupt_record').dropna()

# extract nested coordinates data
df.registerTempTable("raw")
interested_frame = sql_context.sql("SELECT sa2_name16, geometry.coordinates as coordinates FROM raw")
# clean coordinates data

interested_frame.registerTempTable('interested_table')
user_define_function = func.udf(calculate_multipolygon_area, FloatType())
gr = sql_context.sql("SELECT sa2_name16, collect_list(coordinates) AS multipolygon FROM interested_table GROUP BY sa2_name16")

# gr = sql_context.sql("SELECT sa2_name16, collect_list(clean_coordinates) AS multipolygon FROM interested_table GROUP BY sa2_name16")

df1 = gr.withColumn('area', user_define_function('multipolygon')).withColumnRenamed('sa2_name16', 'suburb_name')

# def clean_multipolygon(multipolygon):
#
#     new_multipolygon = []
#     for polygon in multipolygon:
#         new_polygon = []
#         for loop in polygon:
#             if len(loop) < 3:
#                 continue
#
#             new_polygon.extend(loop)
#         if new_polygon:
#             new_multipolygon.append(new_polygon)
#         Polygon(new_polygon)
#
#     return new_multipolygon
#
#
# interested_frame = interested_frame.withColumn('clean_coordinates',
#                                                func.udf(clean_multipolygon,
#                                                         ArrayType(ArrayType(ArrayType(FloatType()))))(interested_frame.coordinates)).drop('coordinates').withColumnRenamed('clean_coordinates', 'coordinates')

#########################################forest dataframe
match = re.compile('(\s[^\s]*)\s')
def convert(value):
    geometry_type, coordinates = value.split(' ', 1)
    geometry_type = geometry_type.strip()
    standardized_coordinates = match.sub(r'\1,', coordinates.strip())
    return f'{geometry_type} {standardized_coordinates}'

schema = StructType([
    StructField('area', FloatType(), nullable=False),
    StructField('polygon', StringType(), nullable=False)
])
df2 = sql_context.read.format("com.databricks.spark.csv").option("header", "false").option('delimiter', ' '). \
    load("/Users/arturogonzalez/PycharmProjects/challenge-urban-forest/melb_urban_forest_2016.txt/part-*",
         schema=schema)
df2 = df2.withColumn('polygon_formatted', func.udf(convert, StringType())('polygon')).drop('polygon').withColumnRenamed('polygon_formatted', 'polygon')


def join_condition(multipolygons, polygon):

    polygon_object = wkt.loads(polygon)

    return may_intersect_modified(multipolygons, polygon_object)


join_condition_udf = func.udf(join_condition, BooleanType())
joined = df1.select(func.col("suburb_name"), func.col("multipolygon"), func.col("area").alias('suburb_area')).crossJoin(df2).where(join_condition_udf(df1.multipolygon, df2.polygon))
joined.registerTempTable('joined_table')
grouped_df = sql_context.sql('select suburb_name, first(multipolygon) as multipolygon, first(suburb_area) as suburb_area, sum(area) as area, collect_list(polygon) as forest_multipolygon from joined_table group by suburb_name')


def calculate_forest_rate(multipolygons, forest_multipolygon, suburb_area):
    merged_suburb = merge_multi_polygons(*multipolygons)
    merge_forest = merge_multi_polygons(*[wkt.loads(m) for m in forest_multipolygon])
    return round(intersection_area(merged_suburb, merge_forest) / suburb_area, 3)


result_df = grouped_df.withColumn('per', func.udf(calculate_forest_rate, FloatType())('multipolygon', 'forest_multipolygon', 'suburb_area')).orderBy(func.desc("per"))
result_df.show()

