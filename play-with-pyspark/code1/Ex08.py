from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import spark_partition_id


'''
Example : Working with DataFrameWriter i.e sink
'''

# --------------------------------------------------------------
# Example 8:  Creating DataFrame From Hive Table
# --------------------------------------------------------------


spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("pyspark-app") \
    .enableHiveSupport() \
    .getOrCreate()


# Read 
DF=spark.sql("SELECT * FROM AIRLINE_DB.flight_data_tbl")
    
# Transformations
# ...


# Write
DF.show(truncate=False)


spark.stop()