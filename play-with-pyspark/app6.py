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
        .appName("app6") \
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
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


    # Transform
    #.....


    # Write
    # Sink: Local FS , Format: JSON
    flightTimeParquetDF=flightTimeParquetDF.coalesce(1)
  
    # flightTimeParquetDF.write \
    #     .format("json") \
    #     .mode("overwrite") \
    #     .partitionBy("ORIGIN") \
    #     .option("maxRecordsPerFile", 500) \
    #     .save("sink/flight-time-json")


    # Sink: MySQL , Format: JDBC

    
    flightTimeParquetDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/flightsdb") \
        .option("dbtable", "flights") \
        .option("user", "root") \
        .option("password", "root1234") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .save()
    

    spark.stop()