from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("PenguinsProcessing") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs:///user/maria_dev/datasets/penguins_clean.csv",
    header=True,
    inferSchema=True
)

df_selected = df.select(
    "species",
    "island",
    "flipper_length_mm",
    "body_mass_g",
    "sex",
    "year"
)

df_filtered = df_selected.filter(
    col("flipper_length_mm") > 0
)

df_sorted = df_filtered.orderBy(
    col("flipper_length_mm").asc()
)

df_sorted.cache()

print("Processed Penguins Data:")
df_sorted.show(10)

df_sorted.printSchema()

print("Total records:", df_sorted.count())

spark.stop()
