import sys
from pyspark import SparkContext
def wc(inputName, outputName):

    context = SparkContext(appName = "Sum", master = "local")

    rdd = context.textFile(inputName)

    word1 = rdd.flatMap(lambda line: line.strip().split())
    wordCount1 = word1.map(lambda word: (word ,1) ).reduceByKey(lambda a,b:a+b)
    wordCount1 = wordCount1.sortBy(lambda a: a[1], False)

    outputFile = []
    for item in wordCount1.take(200):
        outputFile.append(str(item)+'\n')

    x =  context.parallelize(outputFile)

    x.saveAsTextFile(outputName)

def main():
    inputName = sys.argv[1]
    outputName = sys.argv[2]
    wc(inputName, outputName)

if __name__ == '__main__':
    main()

