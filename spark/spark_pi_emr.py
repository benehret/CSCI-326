import boto3
s3 = boto3.resource('s3')
obj = s3.Object("ehretb-326", "output.txt")

import random
from pyspark import SparkContext
import math
context = SparkContext(appName = "Sum", master = "local")

lyst1 = []

for i in range(50000):
    num1 = random.uniform(-1,1)
    num2 = random.uniform(-1,1) 
    datapoint = (num1, num2)
    lyst1.append(datapoint)

rdd = context.parallelize(lyst1)
squarePointsNum = rdd.count()
circlePoints = rdd.filter(lambda x: math.sqrt(x[0]**2 + x[1]**2) <= 1)
circlePointsNum = circlePoints.count()
piEst = circlePointsNum / squarePointsNum * 4

obj.put(Body = str(piEst))

