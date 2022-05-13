from pyspark import SparkContext
context = SparkContext(appName = "Sum", master = "local")

rdd = context.textFile("../../data3/skin_med.txt")

numbers = rdd.map(lambda line: line.strip().split()).map(lambda x: [int(x[0]),int(x[1]),int(x[2])])

numbers0 = numbers.map(lambda x: x[0])
numbers1 = numbers.map(lambda x: x[1])
numbers2 = numbers.map(lambda x: x[2])

numbers0min = numbers0.takeOrdered(1)
numbers0max = numbers0.takeOrdered(1, key = lambda x: -x)
numbers0avg = numbers0.reduce(lambda x,y: x+y)
numbers0average = numbers0avg/numbers0.count()
numbers1min = numbers1.takeOrdered(1)
numbers1max = numbers1.takeOrdered(1, key = lambda x: -x)
numbers1avg = numbers1.reduce(lambda x,y: x+y)
numbers1average = numbers1avg/numbers1.count()
numbers2min = numbers2.takeOrdered(1)
numbers2max = numbers2.takeOrdered(1, key = lambda x: -x)
numbers2avg = numbers2.reduce(lambda x,y: x+y)
numbers2average = numbers2avg/numbers2.count()

print('First column min: '+ str(numbers0min[0]))
print('First column max: '+ str(numbers0max[0]))
print('First column average: '+ str(numbers0average))
print('Second column min: '+ str(numbers1min[0]))
print('Second column max: '+ str(numbers1max[0]))
print('Second column average: '+ str(numbers1average))
print('Third column min: '+ str(numbers2min[0]))
print('Third column max: '+ str(numbers2max[0]))
print('Third column average: '+ str(numbers2average))

