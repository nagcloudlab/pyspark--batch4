
# import pandas as pd

from pyspark.sql import SparkSession
from pyspark import SparkConf

if __name__ == '__main__':

    # df = pd.read_csv('../source/survey_input.csv')
    # print(df.head())
    # print(df.columns)

    conf=SparkConf()
    conf.set("spark.app.name","Survey Analysis")
    conf.set("spark.master","local[3]") # i.e 3 core
  
    spark= SparkSession \
           .builder \
           .config(conf=conf) \
           .getOrCreate()
    
    # Stage-1: Load the data
    survey_df=spark.read \
    .format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .load("../source/survey_input.csv")

    # survey_df.printSchema()
    # survey_df.show(5)

    # get number of partitions
    print(survey_df.rdd.getNumPartitions())

    # re-partition the data
    survey_df = survey_df.repartition(2)

    # get number of partitions
    print(survey_df.rdd.getNumPartitions())

    # record count per partition
    print(survey_df.rdd.glom().map(len).collect())


    # Stage-2: Transformation

    df=survey_df \
    .where("Age<40") \
    .select("Age","Country") \
    .groupBy("Country") \
    .count() 

    # Stage-3: Action ( i.e display, collect, write)

    # display the data
    # df.show()

    # collect the data
    country_count_df=df.collect()

    for row in country_count_df:
        print(row)

    # write the data to file
    # print(country_count_df.rdd.getNumPartitions())
    # country_count_df.write \
    # .format("csv") \
    # .mode("overwrite") \
    # .option("header",True) \
    # .save("../target/survey_output")

    # input("Press Enter")

    # close the spark session
    spark.stop()