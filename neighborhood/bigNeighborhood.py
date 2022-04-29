#Ryan Messick and Bennett Ehret
import os
import math



def neighborhood(file1, newFile):
    lyst = []
    count = 0
    lystNeighborhood = []
    for line in file1:
        x = line.split()
        lyst.append(x)

    for i in lyst:
        for j in lyst:
            if math.sqrt(((int(j[0]) - int(i[0]))**2 + (int(j[1]) - int(i[1]))**2)) <= 25:
                count += 1
        lystNeighborhood.append(count)
        count = 0



    average = 0
    for i in lystNeighborhood:
        average += i
        
    average /= len(lystNeighborhood)
    
    newFile.write("Average amount of neighbors: " + str(average) + "\n")

    maxValue = max(lystNeighborhood)
    
    newFile.write("Max amount of neighbors: " + str(maxValue) + "\n")

    lyst3 = []
    for i in range(10):
        maxValue = lystNeighborhood.index(max(lystNeighborhood))
        max2 = lyst[maxValue]
        lyst3.append(max2)
        lyst.pop(maxValue)
        lystNeighborhood.pop(max(lystNeighborhood))
        
    
    newFile.write("Points with most neighborhoods: " + str(lyst3))
    


def main():
    numbers1 = open(os.path.dirname(__file__) + "/../datasets/numbers1.txt", "r")
    numbers2 = open(os.path.dirname(__file__) + "/../datasets/numbers2.txt", "r")
    newFile3 = open("summary_1.txt", "w")
    newFile2 = open("summary_2.txt", "w")
    neighborhood(numbers1, newFile3)
    neighborhood(numbers2, newFile2)
    print("done")
    numbers1.close()
    numbers2.close()
    newFile2.close()
    newFile3.close()



if __name__ == "__main__":
    main()
    


    
