# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/omarionmaia234@gmail.com/consolidated_pipeline/setup_folder/utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

base_path =f'/Volumes/workspace/dropbox/bronxxe/sportsbar-dp/{data_source}/*.csv'
print(base_path)

df = (
    spark.read.format('csv').
    option("header", "true").
    option("inferSchema", "true").
    load(base_path).
    withColumn("read_timestamp", F.current_timestamp()).
    select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df.limit(10))

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Processing 

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
display(df_bronze.show(10))

# COMMAND ----------

# display(spark.sql("SHOW TABLES IN fmcg.silver"))

# COMMAND ----------

# This code groups the data by customer_id, counts how many times each customer_id appears, and then filters to show only those customer_ids that appear more than once (i.e., duplicates). The result is displayed as a table.

df_duplicates = df_bronze.groupBy("customer_id").count().where("count > 1")
display(df_duplicates)

# COMMAND ----------

# Get all table names in the schema using DataFrame API only
#tables_df = spark.sql("SHOW TABLES IN fmcg.silver").select("tableName")
#tables = [row.tableName for row in tables_df.collect()]

# Drop each table using SQL
#for table in tables:
    #spark.sql(f"DROP TABLE fmcg.silver.{table}")
    #display(fmcg.silver)

# COMMAND ----------

## This code displays all rows from df_silver where the customer_name column has leading or trailing spaces. It compares the original customer_name to a trimmed version and shows rows where they are different.

df_silver = df_bronze.dropDuplicates(["customer_id"])

display(
    df_silver.filter(
        F.col("customer_name") != F.trim(F.col("customer_name"))
    )
)

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

print("Rows after dropping: ", df_silver.count())

# COMMAND ----------

# Your code first replaces city typos using city_mapping, then sets city to None if it is not in the allowed list.

#city_mapping = {
    #'Bengaluruu' : 'Bengaluru',
    #'Bengalore' : 'Bengaluru',

    #'Hyderabadd' : 'Hyderabad',
    #'Hyderbad' : 'Hyderabad',

    #'NewDelhi' : 'New Delhi',
    #'NewDheli' : 'New Delhi',
    #'NewDehlee' : 'New Delhi'
#}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

#df_silver = (
 #   df_silver.replace(city_mapping, subset=["city"])
  #  .withColumn(
   #     "city",
#F.when(F.col("city").isNull(), None)
 #       .when(F.col("city").isin(allowed), F.col("city"))
  #      .otherwise(F.lit(None))
   # )
#)

# COMMAND ----------



# COMMAND ----------

# Your code first replaces city typos using city_mapping, then sets city to None if it is not in the allowed list.

city_mapping = {
    'Bengaluruu' : 'Bengaluru',
    'Bengalore' : 'Bengaluru',

    'Hyderabadd' : 'Hyderabad',
    'Hyderbad' : 'Hyderabad',

    'NewDelhi' : 'New Delhi',
    'NewDheli' : 'New Delhi',
    'NewDehlee' : 'New Delhi'
}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver = (
    ## Uses the city_mapping dictionary to fix known misspellings in the "city" column
    ## The subset parameter ensures only the "city" column is affected
    df_silver.replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        ## Creates a new version of the "city" column with conditional logic:
        ## If the city is NULL, keep it as None
        ## If the city is in the allowed list, keep the value
        ## Otherwise, set it to None (null)
        F.when(F.col("city").isNull(), None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(F.lit(None))
    )
)

df_silver.select("city").distinct().show()

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# Title Case Fix

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap(F.col("customer_name")))
)

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

null_customer_names = ["Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business teams

customer_city_fix =  {
    #Sprintx Nutrition
    789403 : "New Delhi",

    #Zenathlete Foods
    789420 : "Bengaluru",
    789422 : "New Delhi",

    #Primefuel Nutrition
    789521 : "Hyderabad",

    #Recovery Lane
    789603 : "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()]
    , ["customer_id", "new_city"]
)

display(df_fix)

# COMMAND ----------

display(df_silver)

# COMMAND ----------



# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city", 
        F.coalesce(F.col("city"), F.col("new_city"))
    )
    .drop("new_city")
)

# COMMAND ----------

null_customer_names = ["Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business teams

customer_city_fix =  {
    #Primefuel Nutrition
    789521 : "Hyderabad"
}

dff_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()]
    , ["customer_id", "new_city"]
)

display(dff_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(dff_fix, "customer_id", "left")
    .withColumn(
        "city", 
        F.coalesce(F.col("city"), F.col("new_city"))
    )
    .drop("new_city")
)

# COMMAND ----------

null_customer_names = ["Sprintx Nutrition", "Zenathlete Foods", "Primefuel Nutrition", "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = (
    df_silver.withColumn("customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown"))))
    
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
df_silver.printSchema()


# COMMAND ----------

df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .option("mergeSchema", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processing
# MAGIC

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

#take req cols only
#customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel

df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .option("mergeSchema", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

#Preparing target destination table for merge

deltaTable = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

deltaTable.alias("target").merge(
    source = df_child_customers.alias("source"),
    condition = "target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()