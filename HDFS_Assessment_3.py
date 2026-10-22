from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("PenguinsProcessing") \
    .getOrCreate()

try:
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

    df_sorted.show(10)
    print("Total records:", df_sorted.count())

    output_path = (
        "hdfs:///user/maria_dev/spark/job-output/penguins"
    )

    df_sorted.write \
        .mode("overwrite") \
        .partitionBy("species") \
        .json(output_path)

    print("JSON output saved successfully to:", output_path)

finally:
    spark.stop()
