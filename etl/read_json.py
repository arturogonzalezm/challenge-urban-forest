import re

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as func
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import FloatType, StringType, StructType, StructField, MapType, ArrayType, Row, DataType, \
    BooleanType
from shapely import wkt
from shapely.geometry import shape, Polygon, mapping, MultiPolygon

from polygon_utils import multi_polygon_area, merge_multi_polygons, may_intersect, intersection_area, \
    may_intersect_modified, to_shape

config = SparkConf().setAppName('Urban Forest')
conf = (config.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.num.executors', '8')
        # .set('spark.executor.cores', '2')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '10G')).set('default.parallelism', 2)


def calculate_multipolygon_area(multipolygons):
    # areas = []
    # for multipolygon in multipolygons:
    #     areas.append(multi_polygon_area([multipolygon]))
    # return sum(areas)

    merged = merge_multi_polygons(*multipolygons)
    return multi_polygon_area(merged)


def calculate_multipolygon_bounds(multipolygons):
    bounds = []
    for m in multipolygons:
        bounds.append(to_shape(m).bounds)
    return bounds


sc = SparkContext(conf=conf)
# sc = SparkContext.getOrCreate(conf=conf)
# sc.setLogLevel(newLevel)
sql_context = SQLContext(sc)
# read raw data
df = sql_context.read.json(
    '/Users/arturogonzalez/PycharmProjects/challenge-urban-forest/melb_inner_2016.json')
df = df.drop('_corrupt_record').dropna()

# extract nested coordinates data
df.registerTempTable("raw")
interested_frame = sql_context.sql("select sa2_name16 as suburb_name , sa2_main16 as suburb_code, geometry.coordinates as coordinates FROM raw")
# clean coordinates data
interested_frame.registerTempTable('interested_table')
user_define_function = func.udf(calculate_multipolygon_area, FloatType())
gr = sql_context.sql(
    "SELECT suburb_name, first(suburb_code) as suburb_code, collect_list(coordinates) AS multipolygon FROM interested_table GROUP BY suburb_name")

df1 = gr.withColumn('suburb_area', user_define_function('multipolygon'))

multipolygons_bounds_udf = func.udf(calculate_multipolygon_bounds, ArrayType(ArrayType(FloatType())))
df1 = df1.withColumn('suburb_bound', multipolygons_bounds_udf('multipolygon'))



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
df2 = df2.withColumn('polygon_formatted', func.udf(convert, StringType())('polygon')).drop('polygon').withColumnRenamed(
    'polygon_formatted', 'polygon')


def calculate_forest_bound(polygon):
    return wkt.loads(polygon).bounds


calculate_forest_bound_udf = func.udf(calculate_forest_bound, ArrayType(FloatType()))
df2 = df2.withColumn('forest_bound', calculate_forest_bound_udf('polygon'))


def join_condition(suburb_bound, forest_bound):
    def intersect(box_a, box_b):
        a_min_x, a_min_y, a_max_x, a_max_y = box_a
        b_min_x, b_min_y, b_max_x, b_max_y = box_b
        return a_min_y <= b_max_y and \
               a_max_x >= b_min_x and \
               a_max_y >= b_min_y and \
               a_min_x <= b_max_x

    for sub in suburb_bound:
        if intersect(sub, forest_bound):
            return True

    return False


join_condition_udf = func.udf(join_condition, BooleanType())
df1p1 = df1.repartition(1)
df2p1 = df2.repartition(10)
joined = df1p1.crossJoin(df2p1).where(join_condition_udf(df1p1.suburb_bound, df2p1.forest_bound))

joined = joined.drop('suburb_bound').drop('area').drop('forest_bound')

joined.registerTempTable('joined_table')
sql_context.cacheTable("joined_table")
a = sql_context.sql('select count(*) from joined_table')
a.show()
# result = sql_context.sql(
#     'select suburb_code, first(multipolygon) as multipolygon, first(suburb_area) as suburb_area, collect_list(polygon) '
#     'as forest_multipolygon from joined_table group by suburb_code')
# forest_grouped = sql_context.sql(
#     'select suburb_code, collect_list(polygon) as forest_multipolygon from joined_table group by suburb_code')

# joined.groupBy('suburb_code').agg({'polygon': 'collect_list'}).show()

# forest_grouped_p = forest_grouped.repartition(1)
# result = df1p1.join(forest_grouped_p, df1.suburb_name == forest_grouped.suburb_name, how='inner')
# result.show()

# def calculate_forest_rate(multipolygons, forest_multipolygon, suburb_area):
#     raise ValueError('test')
#     merged_suburb = merge_multi_polygons(*multipolygons)
#     merge_forest = merge_multi_polygons(*[wkt.loads(m) for m in forest_multipolygon])
#     return round(intersection_area(merged_suburb, merge_forest) / suburb_area, 3)
#
# result = result.withColumn('per', func.udf(calculate_forest_rate, FloatType())('multipolygon', 'forest_multipolygon', 'suburb_area'))
#
# result.show(n=2)
# result = result.orderBy('per', ascending=False)
# result.show(n=2)
