import sys

from pyspark.sql import SparkSession

def comms(inputName):
    session = SparkSession.builder.appName("Sum").getOrCreate()
    context  = session.sparkContext
    data = session.read.json(inputName)
    data_rdd = data.rdd

    data1rdd = data_rdd.map(lambda row: (row["author"],1)).reduceByKey(lambda a, b: a + b)
    data2rdd = data_rdd.map(lambda row: (row["subreddit"],1)).reduceByKey(lambda a, b: a + b)
    data3rdd = data_rdd.map(lambda row: ((row["author"], row["body"]), row["score"]))
    data5rdd = data_rdd.map(lambda row: (row["author"],row["score"]))
    data1rdd = data1rdd.sortBy(lambda x: x[1], False)  
    data2rdd = data2rdd.sortBy(lambda x: x[1], False)
    data3rdd = data3rdd.reduceByKey(lambda x,y: max(x,y))
    data3rdd = data3rdd.sortBy(lambda x: x[1], False)
    data4rdd = data3rdd.sortBy(lambda x: x[1])
    data5rdd = data5rdd.groupByKey().mapValues(lambda x: sum(x)/len(x))
    data5rdd = data5rdd.sortBy(lambda x: x[1], False)
    data6rdd = data5rdd.sortBy(lambda x: x[1])


    lyst = []
    lyst2 = []
    lyst3 = []
    lyst4 = []
    lyst5 = []
    lyst6 = []



    lyst.append(data1rdd.take(1000))
    lyst2.append(data2rdd.take(1000))
    lyst3.append(data3rdd.take(1000))
    lyst4.append(data4rdd.take(1000))
    lyst5.append(data5rdd.take(1000))
    lyst6.append(data6rdd.take(1000))
    
    outrdd = context.parallelize(lyst)
    outrdd2 = context.parallelize(lyst2)
    outrdd3 = context.parallelize(lyst3)
    outrdd4 = context.parallelize(lyst4)
    outrdd5 = context.parallelize(lyst5)
    outrdd6 = context.parallelize(lyst6)

    outrdd.saveAsTextFile("s3://ehretb-326/top_user_count.txt")
    outrdd2.saveAsTextFile("s3://ehretb-326/top_sub_count.txt")
    outrdd3.saveAsTextFile("s3://ehretb-326/top_user_com.txt")
    outrdd4.saveAsTextFile("s3://ehretb-326/bot_user_com.txt")
    outrdd5.saveAsTextFile("s3://ehretb-326/top_user_score.txt")
    outrdd6.saveAsTextFile("s3://ehretb-326/bot_user_count.txt")


def main():
    inputName = sys.argv[1]
    comms(inputName)

if __name__ == '__main__':
    main()
