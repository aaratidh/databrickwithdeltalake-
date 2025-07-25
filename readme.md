"Absolutely! At Capital One, I noticed that our PySpark ETL jobs processing large financial transaction data were experiencing unpredictable runtimes and sometimes failing due to skewed data partitions.

Instead of simply scaling up clusters (which increases costs), I took an innovative approach. I designed a dynamic partitioning strategy where the job analyzes the distribution of keys before executing transformations.

I built a small pre-processing module in Python that samples the data, calculates approximate record counts per key, and dynamically adjusts partition ranges and sizes based on that distribution. I integrated this module as a reusable component into our Airflow DAGs, so every job could automatically adjust partitions at runtime.

This drastically reduced shuffle-heavy operations, decreased job runtimes by about 35%, and significantly lowered EMR costs.

Moreover, I extended this concept to create a custom Spark listener that tracked stage-level metrics and automatically recommended further optimizations for future runs — something not supported out-of-the-box.

This solution not only improved pipeline efficiency but also empowered the team to adopt a more data-driven approach to job tuning, moving away from guesswork and manual tweaking."



# adding file to aws ()
# conneting to aws 
```python
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
spark.read.csv("s3://databrickproject88/").show()

df = spark.read.json("s3://databrickproject88/enigma.json")
df.show()
# externabla table 
df.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://databrickproject88/enigma-delta/")
```
```sql
%sql

CREATE TABLE if not exists enigma
USING DELTA
LOCATION 's3a://databrickproject88/enigma-delta/'

-- # Writing  creating delta table we need to write using "create table format becase "  sql format Creates a metastore table named enigma.

-- # Points it to the Delta table files at s3a://databrickproject88/enigma-delta/.

-- # This lets you easily query it using SQL (e.g., SELECT * FROM enigma) and use it in notebooks without specifying the path.
```
```python
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
```


 ##  Theory 

Where we can simply download or upload the data in/from datalake  like S3 to into a dataframe and clean and transform data? why is there need of the open table format like Hudi/Iceberg/Delta lake 

Extracting data from Datalake and reading them as a dataframe and performing operation and later saving it as a file in datalake seems like a simple and easy approach for generating clean data? 

Yet,  DataFrames do not support ACID transactions, so performing multiple updates or deletes can potentially corrupt the data. When sharing data, as data engineering team you can't simply share a clean version of a DataFrame, you need a reliable storage format that can maintain and track the clean version over time.
Using plain DataFrames also means that you have to manually rewrite entire files to update data, which is inefficient and error-prone. If the data is overwritten, the original state is lost, making it impossible to query historical versions of the data.
Moreover, when new data arrives with schema changes, you must repeat the entire process:
read from S3 → load into a DataFrame → handle schema changes → clean the data. This process quickly becomes unmanageable as data complexity grows.

example:  There is claim.Json file in S3  and you read it as a data frame

<img width="975" height="161" alt="image" src="https://github.com/user-attachments/assets/304a9d20-4ba9-4abd-8860-7d5798d5d1dd" />

Assume you applied some transformation and clean  

<img width="975" height="204" alt="image" src="https://github.com/user-attachments/assets/6960152d-36a9-4dfa-aced-210c41f4a2f1" />


data  now you are keep the clean file in S3  as clean /claims_cleaned 
 <img width="866" height="131" alt="image" src="https://github.com/user-attachments/assets/45596c85-c1a9-421e-8e94-64e3860ddc70" />


Uploading a DataFrame to S3 is like putting a PDF on Google Drive with no index, you know the data is there, but there’s no way to search, track changes, or connect to it programmatically. Only you will know claims_cleaned has correct data or which is raw file or you have to keep track of those transaction separately which is not a possible while you are dealing with large volume of data and large numbers of files.
<img width="924" height="139" alt="image" src="https://github.com/user-attachments/assets/8cd777af-735e-47fc-829a-fa4e5ce105a5" />
<img width="975" height="126" alt="image" src="https://github.com/user-attachments/assets/d6f9c5e1-9e28-49a9-8af8-e8a578808816" />

Files stored directly in S3 offer no logical layer to handle updates, inserts, versioning, or schema evolution. While you can modify a DataFrame, it doesn't provide the flexibility, governance, or reliability that open table formats offer.
In contrast, using an open table format like Delta Lake, Apache Hudi, or Iceberg is like uploading that same document into a database with metadata, tags, and searchable fields. Now, everyone can find it, query it, manage changes over time, and connect BI tools or downstream systems with confidence.

In opentable format provides ACID transactions on top of S3 using a transaction log (_delta_log in case of delta table), which tracks every operation (e.g., append, overwrite) in a consistent, versioned way.
 
Atomicity: Operations like appending data or overwriting are atomic. Either all data is written successfully or none at all.
Consistency: The table's schema and data remain valid and consistent with every commit.
Isolation: Concurrent operations (e.g., multiple jobs writing/appending) are isolated using optimistic concurrency.
Durability: Once a write is committed, it's reliably stored in S3 and recorded in the Delta log.
 <img width="753" height="242" alt="image" src="https://github.com/user-attachments/assets/1dcaab6f-bc7f-4549-9755-e67fdb7c654d" />


The code in the picture: This safely adds data and registers it in the Delta transaction log.
Create a logical layer and Accessibility
<img width="975" height="244" alt="image" src="https://github.com/user-attachments/assets/1d3719c2-c117-484a-96fa-79ce43dda842" />
 

Creating a logical layer means registering the Delta table in the metastore, so it can be accessed by name instead of file path. This improves accessibility, letting users run SQL queries like SELECT * FROM enigma without referencing S3, making data easier to use and share. But while reading as a dataframe one has to use the complete physical address. 
<img width="805" height="176" alt="image" src="https://github.com/user-attachments/assets/e490b798-1a45-4139-9e66-19b1980826df" />

 
.option("mergeSchema", "true") lets you add new columns to the Delta table during an append without breaking the schema—ideal for evolving datasets.
.option("overwriteSchema", "true") allows you to replace the entire schema when doing an overwrite, useful when the structure of your data has changed completely. Use this carefully to avoid data loss.
These options give data engineers flexibility to handle schema changes easily. .mergeSchema allows adding new columns during appends, supporting evolving data without breaking pipelines. .overwriteSchema lets you fully replace the schema during overwrites, useful for major updates. Together, they make pipelines more robust, automated, and adaptable to changing data.
Delta lake  create version through Table snap shot Delta Lake maintains versioning using the _delta_log folder, which records every table change as a new JSON commit file. Each commit captures metadata, schema changes, and file additions or deletions. This enables features like time travel, where you can query data as of a specific version or timestamp, and rollback, allowing safe recovery from bad writes. It also supports reproducibility and auditability, making data lakes reliable and production-ready.


Hence Delta Lake turns object storage (like S3) into a robust, scalable table format that: Maintains ACID guarantees even in distributed environments.
•	Provides a logical and queryable table layer through metastore registration.
•	Supports evolving schemas for changing data over time.
•	Enables time travel, versioning, and easy integration with ML/BI tools.
This makes Delta a powerful format for building data lakes with data warehouse capabilities.



























