from pyspark.sql import *
from pyspark.sql.functions import *
import re

# --------------------------------------------------------------
# Example 11:  Working with UDF
# --------------------------------------------------------------

# Define UDF
def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"
    

spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("LogFileDemo") \
    .getOrCreate()


survey_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("source/survey.csv")

# Create UDF
# parse_gender_udf = udf(parse_gender, returnType=StringType()) 
# list=[r for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
# print(list)

# survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
# survey_df2.select("Gender").show()

# Register UDF
spark.udf.register("parse_gender_udf", parse_gender, StringType())
# list=[r for r in spark.catalog.listFunctions() if "parse_gender" in r.name]
# print(list)

survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
survey_df3.select("Gender").show()


spark.stop()