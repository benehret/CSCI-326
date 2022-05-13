import sys
from pyspark import SparkContext
def pairs(inputName, outputName):
    context = SparkContext(appName = "Sum", master = "local")

    rdd = context.textFile(inputName)

    word1 = rdd.flatMap(lambda line: line.strip().split())

    wordpairs1 = rdd.map(lambda line: line.split()).flatMap(lambda x: [(x[i], x[i + 1]) for i in range(0, len(x) - 1)])
    wordCount1 = wordpairs1.map(lambda word: (word ,1)).reduceByKey(lambda a,b:a+b)
    wordCount1 = wordCount1.sortBy(lambda a: a[1], False)
    
    outputFile = open(outputName, 'w')
    for item in wordCount1.take(50):
        outputFile.write(str(item)+'\n')

def main():
    inputName = sys.argv[1]
    outputName = sys.argv[2]
    pairs(inputName, outputName)

if __name__ == '__main__':
    main()



