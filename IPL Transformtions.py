# Databricks notebook source
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install snowflake-sqlalchemy

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
spark=SparkSession.builder.appName("basic").getOrCreate()

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True),
    StructField("newCol", IntegerType(), True)
    
])
match_df = spark.read.schema(match_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Match.csv")
match_df.show()

# COMMAND ----------

#match_df.select("outcome_type").distinct().show()
match_df.filter(col("outcome_type")=="abandoned").show()

# COMMAND ----------

matches_played = match_df.selectExpr("team1 as team", "season_year").union(
    match_df.selectExpr("team2 as team", "season_year")
).groupby("season_year", "team").agg(count("*").alias("matches_played"))

# Count Wins (Group by Season & Team)
wins = match_df.groupBy("season_year", "match_winner").agg(count("*").alias("wins")) \
    .withColumnRenamed("match_winner", "team")

# Count No Results (Group by Season & Team)
no_results = match_df.filter(col("outcome_type").isin("no result","abandoned")) \
    .selectExpr("team1 as team", "season_year").union(
    match_df.filter(col("outcome_type").isin("no result","abandoned")).selectExpr("team2 as team", "season_year")
).groupBy("season_year", "team").agg(count("*").alias("no_results"))

# Count tied Matches (Gropy by Season & Team) 
tied=match_df.filter(col("outcome_type")=="tied").selectExpr("team1 as team","season_year").union(
    match_df.filter(col("outcome_type")=="tied").selectExpr("team2 as team", "season_year")
).groupBy("season_year","team").agg(count("*").alias("tied"))

# Merge Data
points_table = matches_played.join(wins, ["season_year", "team"], "left") \
    .join(no_results, ["season_year", "team"], "left").fillna(0)\
    .join(tied, ["season_year", "team"], "left").fillna(0)

# Calculate Losses & Points
points_table = points_table.withColumn("losses", col("matches_played") - col("wins") - col("no_results")) \
    .withColumn("points", (col("wins") * 2) + col("no_results") + col("tied"))

# Show IPL Points Table sorted by Season and Points

result =points_table.orderBy(col("season_year").desc(), col("points").desc()) \
    .coalesce(1)

result.show()

# COMMAND ----------

snowflake_options = {
    "sfURL": "SNOWFLAKE LINK",
    "sfDatabase": "YOUR DB",
    "sfSchema": "YOUR SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",  # Optional
    "sfUser": "USER NAME",
    "sfPassword": "PASSWORD"
}

# COMMAND ----------

result.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "TEAM_PERFORMANCE") \
    .mode("append") \
    .save()

# COMMAND ----------

