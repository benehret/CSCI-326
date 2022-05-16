import sys

from pyspark import SparkContext
def numbers(inputName,outputName):

    context = SparkContext(appName = "Sum", master = "local")

    rdd = context.textFile(inputName)
    
    numbers = rdd.map(lambda line: line.strip().split(',')[0:6])

    def findMin(x,y):
        newMins = []
        for i in range(6):
            newMins.append(min(float(x[i]),float(y[i])))
        return newMins
    
    def findMax(x,y):
        newMaxs = []
        for i in range(6):
            newMaxs.append(max(float(x[i]),float(y[i])))
        return newMaxs
    def findSum(x,y):
        newSums = []
        for i in range(6):
            newSums.append(float(x[i]) + float(y[i]))
        return newSums
    numbersMin = numbers.reduce(findMin)
    numbersMax = numbers.reduce(findMax)
    numbersSums = numbers.reduce(findSum)
    size = int(numbers.count())

    for i in range(6):
        numbersSums[i] = numbersSums[i]/size

    olist = []

    olist.append('Column Mins: '+ str(numbersMin))
    olist.append('Column Maxs: '+ str(numbersMax))
    olist.append('Column Averages: '+ str(numbersSums))

    outrdd = context.parallelize(olist)
    outrdd.saveAsTextFile(outputName)

def main():
    inputName = sys.argv[1]
    outputName = sys.argv[2]
    numbers(inputName, outputName)


if __name__ == '__main__':
    main()
