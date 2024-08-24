# ASSIGNMENT 1

Load NYC taxi data to DataLake/Blob_Storage/DataBricks and extract the data through dataframe in the notebook. 

Perform Following **Queries using PySpark**. 

**Query 1.** - Add a column named as ""`Revenue`"" into dataframe which is the sum of the below columns '`Fare_amount`','`Extra`','`MTA_tax`','`Improvement_surcharge`','`Tip_amount`','`Tolls_amount`','`Total_amount`' 

**Query 2.** - Increasing count of total passengers in New York City by area 

**Query 3.** - Realtime Average fare/total earning amount earned by 2 vendors 

**Query 4.** - Moving Count of payments made by each payment mode 

**Query 5.** - Highest two gaining vendor's on a particular date with no of passenger and total distance by cab 

**Query 6.** - Most no of passenger between a route of two location. 

**Query 7.** - Get top pickup locations with most passengers in last 5/10 seconds."


### Resources :
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml 

Choose `Trip sheet data -> 2018 -> January -> yellow` 

Type https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv




# Solution

## <span style="color:red">**NOTE**</span>

The provided [link](https://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) for the trip_record shows

```
Taxi & Limousine Commission has recently redesigned its website and this page has moved. Please update your bookmark to:

https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

You will be redirected in 5 seconds, or click on the link above.
```

### The actual link (🤔Probably) to download can be [found here](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-01.parquet)

The second link for [yellow_tripdata_2020-01.csv](https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv) shows permission denied as follows :-

```xml
This XML file does not appear to have any style information associated with it. The document tree is shown below.
<Error>
<Code>AccessDenied</Code>
<Message>Access Denied</Message>
<RequestId>WPZX6J4XDJNWK8YN</RequestId>
<HostId>x3zXQTO6jLgQg4Q232lwGnmIeYCksbGLCzPcXqKKGHxm3TDKguWcS65zd6g9q54LgPe2y/OId8E=</HostId>
</Error>
```
 

#### <span style="color:red">PySpark was not covered in the current course week and this assignment seem unrelated to current content being taught hence referred to google and chatGPT for the assignment</span>


## Steps to approach the problem

### Step 1 :**Upload the CSV File** 

Once the file is downloaded from the new link, upload it to a container in our Azure storage account.


Read basic of Pyspark [here](https://www.datacamp.com/tutorial/pyspark-tutorial-getting-started-with-pyspark)

### Step 2 : Loading data with PySpark

```py
from pyspark.sql import SparkSession
# Load if exists or create a new one
spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .getOrCreate()

# Load data into DataFrame
df = spark.read.option("header", "true").csv("/mnt/nyc-taxi/yellow_tripdata_2020-01.csv")
```

### Step 3 : Queries

#### Query 1 solution
Add a Revenue Column
```py
from pyspark.sql.functions import col
# revenue = fare_amount + extra + mta_tax + improvement_surcharge + trip_amount + tolls_amount + total_amount
df = df.withColumn("Revenue", col("Fare_amount") + col("Extra") + col("MTA_tax") +
                   col("Improvement_surcharge") + col("Tip_amount") + col("Tolls_amount") + col("Total_amount"))
df.show()
```

#### Query 2 solution
Increasing Count of Total Passengers by Area
```py
df.groupBy("PULocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "total_passengers").orderBy("total_passengers", ascending=False).show()

```

#### Query 3 solution
Average Fare/Total Earning Amount Earned by 2 Vendors
```py
df.groupBy("VendorID").avg("total_amount").withColumnRenamed("avg(total_amount)", "average_earning").show()

```

#### Query 4 solution
Count of Payments Made by Each Payment Mode
```py
df.groupBy("payment_type").count().withColumnRenamed("count", "payment_count").show()

```

#### Query 5 solution
Highest Two Gaining Vendors on a Particular Date with No of Passengers and Total Distance by Cab
```py
from pyspark.sql.functions import to_date

specific_date = "2020-01-15"
df_filtered = df.filter(to_date(df.tpep_pickup_datetime) == specific_date)
df_filtered.groupBy("VendorID").agg(
    {"total_amount": "sum", "passenger_count": "sum", "trip_distance": "sum"}
).withColumnRenamed("sum(total_amount)", "total_earning").withColumnRenamed("sum(passenger_count)", "total_passengers").withColumnRenamed("sum(trip_distance)", "total_distance").orderBy("total_earning", ascending=False).show(2)

```

#### Query 6 Solution
Most No of Passengers Between a Route of Two Locations

```py
df.groupBy("PULocationID", "DOLocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "total_passengers").orderBy("total_passengers", ascending=False).show(1)
```


#### Query 7 Solution
Top Pickup Locations with Most Passengers in Last 5/10 Second
```py
from pyspark.sql.functions import unix_timestamp, window

# Assuming current_time is the latest timestamp in the data
current_time = df.select(unix_timestamp(col("tpep_pickup_datetime"))).orderBy(unix_timestamp(col("tpep_pickup_datetime")), ascending=False).first()[0]

df.withColumn("pickup_time", unix_timestamp(col("tpep_pickup_datetime"))).filter(col("pickup_time") > (current_time - 10)).groupBy("PULocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "total_passengers").orderBy("total_passengers", ascending=False).show()
```

