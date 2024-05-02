
from pyspark import SparkContext, SparkConf
from collections import namedtuple


# --------------------------------------------
# Example 2: Spark Application with RDD
# --------------------------------------------


# namedtuple
SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])


if __name__ == "__main__":

    conf = SparkConf().setAppName("App2").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    # read
    linesRDD = sc.textFile("./source/survey.csv")


    # rdd, provides Higher-Oder functions

    # HOF : map, filter, reduceByKey, collect, etc..


    #transform
    partitionedRDD = linesRDD.repartition(2)
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    surveyRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = surveyRDD.filter(lambda rec: rec.Age < 40)
    kvRDD = filteredRDD.map(lambda rec: (rec.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda x, y: x + y)

    # write
    colsList = countRDD.collect()
    for col in colsList:
        print(col)


