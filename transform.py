from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def transform():
    spark = SparkSession.builder.appName("GitHubDataTransform").getOrCreate()

    # read json extracted files
    df = spark.read.option("multiline","true").json("github_extract/*.json")
    df.show(truncate=False)
    # transformation
    df_tranformed = df.select(
        col("head.repo.full_name").alias("Organization Name"),
        col("head.repo.id").alias("repository_id"),
        col("head.repo.name").alias("repository_name"),
        col("head.repo.owner.login").alias("repository_owner"),
        F.regexp_extract("_links.html.href", "/pull/(\\d+)", 1).cast(IntegerType()).alias("num_prs"),
        F.regexp_extract("_links.commits.href", "/pulls/(\\d+)/", 1).cast(IntegerType()).alias("num_prs_merged"),
        col("merged_at")
    )

    result = df_tranformed.withColumn('is_compliant', expr("CASE WHEN num_prs = num_prs_merged \
        AND LOWER(repository_owner) LIKE '%scytale%' THEN 1 ELSE 0 end"))

    result.show(truncate=False)

    # saving to parquet
    result.write.parquet("data/result.parquet", mode="overwrite")
    spark.stop()
