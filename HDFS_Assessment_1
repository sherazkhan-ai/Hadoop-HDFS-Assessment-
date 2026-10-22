# Hadoop-HDFS-Assessment-1
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PenguinsSchema") \
    .getOrCreate()

df = spark.read.csv(
    "hdfs:///user/maria_dev/datasets/penguins_clean.csv",
    header=True,
    inferSchema=True
)

df.printSchema()

spark.stop()
