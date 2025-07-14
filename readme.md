"Absolutely! At Capital One, I noticed that our PySpark ETL jobs processing large financial transaction data were experiencing unpredictable runtimes and sometimes failing due to skewed data partitions.

Instead of simply scaling up clusters (which increases costs), I took an innovative approach. I designed a dynamic partitioning strategy where the job analyzes the distribution of keys before executing transformations.

I built a small pre-processing module in Python that samples the data, calculates approximate record counts per key, and dynamically adjusts partition ranges and sizes based on that distribution. I integrated this module as a reusable component into our Airflow DAGs, so every job could automatically adjust partitions at runtime.

This drastically reduced shuffle-heavy operations, decreased job runtimes by about 35%, and significantly lowered EMR costs.

Moreover, I extended this concept to create a custom Spark listener that tracked stage-level metrics and automatically recommended further optimizations for future runs — something not supported out-of-the-box.

This solution not only improved pipeline efficiency but also empowered the team to adopt a more data-driven approach to job tuning, moving away from guesswork and manual tweaking."



# adding file to aws ()
# conneting to aws 

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
spark.read.csv("s3://databrickproject88/").show()

df = spark.read.json("s3://databrickproject88/enigma.json")
df.show()
# externabla table 
df.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://databrickproject88/enigma-delta/")
%sql
CREATE TABLE if not exists enigma
USING DELTA
LOCATION 's3a://databrickproject88/enigma-delta/'

-- # Writing  creating delta table we need to write using "create table format becase "  sql format Creates a metastore table named enigma.

-- # Points it to the Delta table files at s3a://databrickproject88/enigma-delta/.

-- # This lets you easily query it using SQL (e.g., SELECT * FROM enigma) and use it in notebooks without specifying the path.

df = spark.table("enigma")
df.show()

# spark.table("enigma") looks up the table enigma in the Databricks metastore (which you created with CREATE TABLE enigma ...).

# It automatically knows the Delta location (s3a://databrickproject88/enigma-delta/) because you defined it when creating the table.

#You do not need to specify .format("delta") or the S3 path again.
from pyspark.sql.functions import current_timestamp, when, col, lit

def add_audit_columns(df, is_new=True):
    """
    Add or update audit columns: created_at and updated_at.

    Parameters:
    - df: DataFrame to process
    - is_new: True if inserting new rows, False if updating existing rows

    Returns:
    - DataFrame with audit columns
    """
    if is_new:
        # New rows: set both created_at and updated_at to now
        df = df.withColumn("created_at", current_timestamp()) \
               .withColumn("updated_at", current_timestamp())
    else:
        # Updating rows: leave created_at as is, update updated_at
        if "created_at" not in df.columns:
            df = df.withColumn("created_at", lit(None).cast("timestamp"))
        df = df.withColumn("updated_at", current_timestamp())

    return df
df = add_audit_columns(df)
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("s3a://databrickproject88/enigma-delta/")
df = spark.table("enigma")
df.show(truncate= False)

df = df.na.fill({"recovered": "0"})
df.show()

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://databrickproject88/enigma-delta/")

df = spark.table("enigma")
df.show()
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("active", StringType(), True),
    StructField("admin2", StringType(), True),
    StructField("combined_key", StringType(), True),
    StructField("confirmed", IntegerType(), True),
    StructField("country_region", StringType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("fips", StringType(), True),
    StructField("last_update", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("province_state", StringType(), True),
    StructField("recovered", StringType(), True),
])

data = [
    Row(active=None, admin2=None, combined_key="Anhui, China", confirmed=1, country_region="China", deaths=None, fips=None, last_update="2020-01-22T17:00:00", latitude=31.826, longitude=117.226, province_state="Anhui", recovered="0"),
    Row(active=None, admin2=None, combined_key="Beijing, China", confirmed=14, country_region="China", deaths=None, fips=None, last_update="2020-01-22T17:00:00", latitude=40.182, longitude=116.414, province_state="Beijing", recovered="0"),
]

df2 = spark.createDataFrame(data, schema=schema)
df2.show(truncate=False)

df2.write.format("delta").mode("append").saveAsTable("enigma")

   df2.printSchema()


spark.table("enigma").printSchema()
# there is difference in the schema so it is showing error   
# creating new sample data that matches schema with which is present in the enigma table
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

schema = StructType([
    StructField("active", LongType(), True),
    StructField("admin2", StringType(), True),
    StructField("combined_key", StringType(), True),
    StructField("confirmed", LongType(), True),
    StructField("country_region", StringType(), True),
    StructField("deaths", LongType(), True),
    StructField("fips", StringType(), True),
    StructField("last_update", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("province_state", StringType(), True),
    StructField("recovered", LongType(), True),
])

data = [
    Row(active=None, admin2=None, combined_key="Anhui, China", confirmed=1, country_region="China", deaths=None, fips=None, last_update="2020-01-22T17:00:00", latitude=31.826, longitude=117.226, province_state="Anhui", recovered=0),
    Row(active=None, admin2=None, combined_key="Beijing, China", confirmed=14, country_region="China", deaths=None, fips=None, last_update="2020-01-22T17:00:00", latitude=40.182, longitude=116.414, province_state="Beijing", recovered=0),
]

df2 = spark.createDataFrame(data, schema=schema)
df2.show(truncate=False)

# using cast to handle schema mis match 
# Get target table schema
target_df = spark.table("enigma")
target_schema = target_df.schema

# Function to cast new dataframe
from pyspark.sql.functions import lit, col

def align_schema(new_df, target_schema):
    for field in target_schema:
        if field.name not in new_df.columns:
            # Add missing columns with nulls
            new_df = new_df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            # Cast to match type
            new_df = new_df.withColumn(field.name, col(field.name).cast(field.dataType))
    # Only keep columns that exist in target
    new_df = new_df.select([f.name for f in target_schema])
    return new_df

# Align your new DataFrame
df2_aligned = align_schema(df2, target_schema)

# Now append safely
df2_aligned.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("enigma")
# checking if it has correct schema 

df = spark.table("enigma")
df.show()
spark.table("enigma").orderBy("last_update", ascending=False).show(10, truncate=False)

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "enigma")
history_df = delta_table.history()
history_df.show(truncate=False)

