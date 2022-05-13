import sys
from pyspark import SparkContext
def wc(inputName, outputName):

    context = SparkContext(appName = "Sum", master = "local")

    rdd = context.textFile(inputName)

    word1 = rdd.flatMap(lambda line: line.strip().split())
    wordCount1 = word1.map(lambda word: (word ,1) ).reduceByKey(lambda a,b:a+b)
    wordCount1 = wordCount1.sortBy(lambda a: a[1], False)

    outputFile = open(outputName, 'w')
    for item in wordCount1.take(50):
        outputFile.write(str(item)+'\n')

def main():
    inputName = sys.argv[1]
    outputName = sys.argv[2]
    wc(inputName, outputName)

if __name__ == '__main__':
    main()


