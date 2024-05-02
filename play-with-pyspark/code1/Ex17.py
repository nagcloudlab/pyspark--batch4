
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

#----------------------------------------------------------
# Example 17: Broadcast Join
#----------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Join Demo") \
        .master("local[3]") \
        .getOrCreate()
    

    flight_time_df1 = spark.read.json("source/d1/")
    flight_time_df2 = spark.read.json("source/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    join_df.collect()
    input("press a key to stop...")


    spark.stop()


    