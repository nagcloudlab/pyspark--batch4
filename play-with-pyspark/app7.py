from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import spark_partition_id


'''
Example : Working with DataFrameWriter i.e sink
'''

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .enableHiveSupport() \
        .getOrCreate()
    

    # Read 
    flightTimeParquetDF=spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.parquet")
    
    print("Num of partitions before re-parttiion: ", flightTimeParquetDF.rdd.getNumPartitions())
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    # Repartition
    flightTimeParquetDF = flightTimeParquetDF.repartition(2)

    print("Num of partitions after re-parttiion: ", flightTimeParquetDF.rdd.getNumPartitions())
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()
  

    # Transformations
    # ...

    
    flightTimeParquetDF=flightTimeParquetDF.coalesce(1)


    # drop the table if exists
    spark.sql("DROP TABLE IF EXISTS AIRLINE_DB.flight_data_tbl")
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")


    # Write
    flightTimeParquetDF.write \
        .format("parquet") \
        .bucketBy(5, "OP_CARRIER") \
        .sortBy("OP_CARRIER") \
        .saveAsTable("flight_data_tbl")
    
    print("Tables: ", spark.catalog.listTables("AIRLINE_DB"))
    
    spark.stop()