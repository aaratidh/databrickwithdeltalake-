
# conneting to aws 
```python
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "#######")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "#######")
df = spark.read.json("s3://databrickproject88/enigma.json")
df.printSchema()
df.show()
%
```
```python

# Creating a surogate key "row_hash" and audit_metrics for delta table to capture change in delta table

from pyspark.sql.functions import current_timestamp, lit, md5, concat_ws

df = df.withColumn("row_hash", md5(concat_ws("||", "province_state", "country_region", "latitude", "longitude","last_update"))) \
       .withColumn("valid_from", current_timestamp()) \
       .withColumn("valid_to", lit(None).cast("timestamp")) \
       .withColumn("is_current", lit(True))


```

```python

# Register the incoming DataFrame as a temp view
df.createOrReplaceTempView("staging_data")

# Check if Delta table exists
from delta.tables import DeltaTable

delta_path = "s3://databrickproject88/delta/enigma_cdc/"

if DeltaTable.isDeltaTable(spark, delta_path):
    # Register Delta table as a temp view
    spark.read.format("delta").load(delta_path).createOrReplaceTempView("target_table")

    # Step 1: Update existing current rows to not current
    spark.sql("""
        MERGE INTO target_table t
        USING staging_data s
        ON t.row_hash = s.row_hash AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.valid_to = current_timestamp(),
            t.is_current = false
    """)

    # Step 2: Insert new or changed rows
    spark.sql("""
        MERGE INTO target_table t
        USING staging_data s
        ON t.row_hash = s.row_hash AND t.is_current = false
        WHEN NOT MATCHED THEN INSERT *
    """)

else:
    # Initial load
    df.write.format("delta").mode("overwrite").save(delta_path)


```
