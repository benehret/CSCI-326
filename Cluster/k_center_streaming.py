import boto3
import sys
import math
import random

def center(inputFileName, outputname, k):
    c = []
    d = 0
    cp = []
    startAdd = True
    s3 = boto3.resource('s3')
    obj = s3.Object("326-data-bucket", inputFileName)
    obj_dict = obj.get()
    b_stream = obj_dict["Body"]
    it = b_stream.iter_lines(chunk_size = 1024)
    for line in it:
        line = line.decode()
        point = line.split(',')[6:11]
        if len(c) < k:
            c.append(point)
            if len(c) == k and startAdd == True:
                for i in range(len(c)):
                    for j in range(len(c)):
                        tempD = findDistance(c[i],c[j])
                        if d < tempD and i != j:
                            d = tempD
                startAdd = False
        else:
            closestDs = []
            for i in range(len(c)):
                closestDs.append(findDistance(point,c[i]))
            closestD = min(closestDs)
            if closestD >= 2*d:
                c.append(point)
                cp = []
                dp = 2*d
                centersToAdd = True
                cp.append(c[random.randint(0,len(c)-1)])
                while centersToAdd == True:
                    added = False
                    for i in range(len(c)):
                        dList = []
                        for j in range(len(cp)):
                            dList.append(findDistance(c[i],cp[j]))
                        newD = min(dList)
                        if newD > dp:
                            if (c[dList.index(newD)] not in cp):
                                cp.append(c[dList.index(newD)])
                                added = True
                    if added == False:
                        centersToAdd = False
                c = cp
                d = dp
    newfile = open(outputname, "w")
    newfile.write(str(d)+'\n')
    newfile.write(str(c))
    newfile.close()

def findDistance(x,y):
    sums = 0
    for i in range(len(x)):
        sums += (float(x[i]) - float(y[i]))**2
    return math.sqrt(sums)

def main():
    inputname = sys.argv[1]
    outputname = sys.argv[2]
    k = int(sys.argv[3])
    center(inputname, outputname, k)

if __name__ == '__main__':
    main()
