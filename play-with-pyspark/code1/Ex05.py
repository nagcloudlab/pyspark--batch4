
from pyspark.sql import SparkSession


#-----------------------------------------------------------
# Example 5: Read data from MySQL table
#-----------------------------------------------------------

spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("app4") \
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
    .getOrCreate()


# Read data from MySQL table 
todosDF=spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/todosdb") \
    .option("dbtable", "todos") \
    .option("user", "root") \
    .option("password", "root1234") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

todosDF.printSchema()
todosDF.show(5)

# Transformations 

# Write

spark.stop()