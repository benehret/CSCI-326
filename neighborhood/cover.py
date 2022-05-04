#Ryan Messick and Bennett Ehret

import os
import math
import sys


def neighborhood(file1, newFile, distance):
    lyst = []
    count = 0
    lystCover = []
    for line in file1:
        x = line.split()
        lyst.append(x)


    for i in lyst:
        for j in lyst:
            if math.sqrt(((int(j[0]) - int(i[0]))**2 + (int(j[1]) - int(i[1]))**2)+(int(j[2]) - int(i[2]))**2) <= int(distance):
                if j!= i:
                    lyst.pop(lyst.index(j))   
        lystCover.append(i)
        lyst.pop(lyst.index(i))


    for i in range (len(lystCover)):
        newFile.write(str(lystCover[i]) + "\n")




def main():
    #numbers1 = open(os.path.dirname(__file__) + "/../datasets/numbers1.txt", "r")
    #numbers2 = open(os.path.dirname(__file__) + "/../datasets/numbers2.txt", "r")
    #newFile3 = open("cover_1.txt", "w")
    #newFile2 = open("cover_2.txt", "w")
    #neighborhood(numbers1, newFile3)
    #neighborhood(numbers2, newFile2)

    #print("done")
    #numbers1.close()
    #numbers2.close()
    #newFile2.close()
    #newFile3.close()

    #code to use command line args
    inputName = sys.argv[1]
    outputName = sys.argv[2]
    distance = sys.argv[3]
    inputFile = open(str(inputName),"r")
    outputFile = open(str(outputName), "w")
    neighborhood(inputFile, outputFile, distance)


if __name__ == "__main__":
    main()
