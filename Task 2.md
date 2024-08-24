# ASSIGNMENT 2

### 1. Load any dataset into DBFS 
### 2. Flatten JSON fields 
### 3. Write flattened file as external parquet table

# Solution

### <span style="color:red"> NOTE</span>
<span style="color:red"> 
DataBricks was not a part of current course week hence referred to chatGPT and google to understand the assignemnt and provide necessary steps 
</span>

## Steps to approach the problem

### Step 1: Load Any Dataset into DBFS

First, we'll upload a dataset to DBFS. Assume name of file as `test_data.json`

### Step 2 : Read the JSON file into a DataFrame
We use pyspark for following operation
```py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flatten JSON").getOrCreate()

# Path to the JSON file in DBFS
json_file_path = "/dbfs/FileStore/tables/test_data.json"

# Read the JSON file into a DataFrame
df = spark.read.json(json_file_path)
df.show()
```


### Step 3. Flatten JSON Fields

To flatten JSON fields, we'll use the `select` method along with the `alias` method to rename columns. Here, we'll assume our JSON has nested fields.

First we will inspect the schema
```py
df.printSchema()
```
Assume the following schema structure:-
```
# root
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- address: struct (nullable = true)
#  |    |-- street: string (nullable = true)
#  |    |-- city: string (nullable = true)
#  |    |-- state: string (nullable = true)
#  |-- orders: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- order_id: long (nullable = true)
#  |    |    |-- amount: double (nullable = true)
```

To flatten JSON fields
```py
from pyspark.sql.functions import col
flattened_df = df.select(
    col("id"),
    col("name"),
    col("address.street").alias("address_street"),
    col("address.city").alias("address_city"),
    col("address.state").alias("address_state"),
    col("orders.order_id").alias("order_id"),
    col("orders.amount").alias("order_amount")
)
flattened_df.show()
```

### Step 4: Write Flattened File as External Parquet Table
Write the DataFrame as a Parquet file
```py
# Define the path to save the parquet file
parquet_file_path = "/mnt/your_mount_point/flattened_data"

# Write the DataFrame to Parquet format
flattened_df.write.mode("overwrite").parquet(parquet_file_path)
```

Write the DataFrame as a Parquet file
```py
# Register the Parquet file as a table in the Databricks metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS flattened_data
    USING PARQUET
    LOCATION '{parquet_file_path}'
""")
```