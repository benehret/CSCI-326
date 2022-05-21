import sys
from pyspark.sql import SparkSession
import math

def centers(inputName,outputName, k):
    session = SparkSession.builder.appName("Sum").getOrCreate()
    context  = session.sparkContext
    rdd = context.textFile(inputName)
    if inputName[-3:] == 'csv':
        numbers = rdd.map(lambda line: line.strip().split(',')[11:16])
    else:
        numbers = rdd.map(lambda line: line.strip().split())
    c = []
    
    def findDistance(x,y):
        sums = 0
        for i in range(len(x)):
            sums += (float(x[i]) - float(y[i]))**2
        return math.sqrt(sums)

    c = numbers.takeSample(False,1)
    for i in range(k-1):
        tempnumbers = numbers.map(lambda x: (x,min(findDistance(x,center) for center in c)))
        newC, dist = tempnumbers.takeOrdered(1, lambda x: -x[1])[0]
        #print(newC[0][0])
        c.append(newC)
        
    
    oList = []
    oList.append("Centers: " + str(c)+ '\n')
    tempnumbers = numbers.map(lambda x: (x,min(findDistance(x,center) for center in c)))
    newC, dist = tempnumbers.takeOrdered(1, lambda x: -x[1])[0]
    oList.append("Objective: " + str(dist))
    #print(str(c))
    #print(dist)
    outrdd = context.parallelize(oList)
    outrdd.saveAsTextFile(outputName)

def main():
    inputName = sys.argv[1]
    k = int(sys.argv[3])
    outputName = sys.argv[2]
    centers(inputName, outputName, k)

if __name__ == '__main__':
    main()
