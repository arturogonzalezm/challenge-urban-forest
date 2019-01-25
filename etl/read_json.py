import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import collect_list, udf
from pyspark.sql.types import FloatType
from polygon_utils import multi_polygon_area, merge_multi_polygons

# conf = (pyspark.SparkConf().setAppName('Urban Forest').setMaster("local[2]"))


# def calculate_multipolygon_area(multipolygon):
#     # merged = multi_polygon_area(multipolygons)
#     # return multi_polygon_area(merged)
#     return 1
#
#
#
sc = pyspark.SparkContext.getOrCreate()

sql_context = SQLContext(sc)
# df = sql_context.read.json(
#     '/Users/arturogonzalez/PycharmProjects/challenge-urban-forest/melb_inner_2016.json')
# df = df.drop('_corrupt_record').dropna()
# # df.printSchema()
# user_define_function = udf(calculate_multipolygon_area, FloatType())
# gr = df.groupBy('sa2_name16').agg(collect_list('geometry').alias('multipolygon'))
#
# gr_area = gr.rdd.map(f=calculate_multipolygon_area)
# gr_area_df = sql_context.createDataFrame([1,2])
# gr_area_df.show()
# gr_area.show()

columns = ['id', 'dogs', 'cats']
vals = [
     (1, 2, 0),
     (2, 0, 1)
]

df = sql_context.createDataFrame(vals, columns)
df.show()
