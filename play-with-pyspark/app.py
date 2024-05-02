from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, ArrayType


spark = SparkSession \
    .builder \
    .appName("File Streaming Demo") \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# -------------------------------------------------------------------------------------



# READ
kafka_df=spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "invoices") \
.option("startingOffsets", "earliest") \
.load() 

kafka_df.printSchema()

# TRANSFORM

schema = StructType([
    StructField("InvoiceNumber", StringType()),
    StructField("CreatedTime", LongType()),
    StructField("StoreID", StringType()),
    StructField("PosID", StringType()),
    StructField("CashierID", StringType()),
    StructField("CustomerType", StringType()),
    StructField("CustomerCardNo", StringType()),
    StructField("TotalAmount", DoubleType()),
    StructField("NumberOfItems", IntegerType()),
    StructField("PaymentMethod", StringType()),
    StructField("CGST", DoubleType()),
    StructField("SGST", DoubleType()),
    StructField("CESS", DoubleType()),
    StructField("DeliveryType", StringType()),
    StructField("DeliveryAddress", StructType([
        StructField("AddressLine", StringType()),
        StructField("City", StringType()),
        StructField("State", StringType()),
        StructField("PinCode", StringType()),
        StructField("ContactNumber", StringType())
    ])),
    StructField("InvoiceLineItems", ArrayType(StructType([
        StructField("ItemCode", StringType()),
        StructField("ItemDescription", StringType()),
        StructField("ItemPrice", DoubleType()),
        StructField("ItemQty", IntegerType()),
        StructField("TotalValue", DoubleType())
    ]))),
])

value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

# value_df.printSchema()

# value_df.printSchema()
notification_df = value_df \
    .where("value.CustomerType == 'PRIME'") \
    .select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount") \
    .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

# notification_df.printSchema()



# # Write -> console
# notification_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()


# Write -> Kafka
kafka_target_df = notification_df.selectExpr("InvoiceNumber as key",
                                                 """to_json(named_struct(
                                                'CustomerCardNo', CustomerCardNo,
                                                'TotalAmount', TotalAmount,
                                                'EarnedLoyaltyPoints', TotalAmount * 0.2)) as value""")

# WRITE
notification_writer_query = kafka_target_df \
    .writeStream \
    .queryName("Notification Writer") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "notifications") \
    .outputMode("append") \
    .option("checkpointLocation", "chk-point-dir") \
    .start() \

notification_writer_query.awaitTermination()

# -------------------------------------------------------------------------------------