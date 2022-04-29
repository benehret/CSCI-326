#Ryan Messick and Bennett Ehret
import os
import math



def nn(file1, file2, outputFile):
    lyst = []
    lyst2 = []
    maxValue = 10000000
    count = 0
    lystAnswer = []
    for line in file1:
        x = line.split()
        lyst.append(x)

    for line in file2:
        y = line.split()
        lyst2.append(y) 

    for i in lyst:
        for j in lyst2:
            num = math.sqrt(((int(j[0]) - int(i[0]))**2 + (int(j[1]) - int(i[1]))**2))
            if num < maxValue:
                maxValue = num
                lystAnswer.append(j)

        maxValue = 10000000
        outputFile.write(str(i) + "->" + str(lystAnswer[len(lystAnswer) - 1]))
        lystAnswer = []
        outputFile.write("\n")
            
    
            
            
            



    


def main():
    numbers1 = open(os.path.dirname(__file__) + "/../datasets/numbers1.txt", "r") 
    numbers3 = open(os.path.dirname(__file__) + "/../datasets/testNumbers.txt", "r")
    num1 = open("out_numbers1.txt", "w")
    nn(numbers3, numbers1, num1)
    numbers1.close()
    num1.close()
    numbers3.close()
    numbers3 = open(os.path.dirname(__file__) + "/../datasets/testNumbers.txt", "r")
    numbers2 = open(os.path.dirname(__file__) + "/../datasets/numbers2.txt", "r")
    num2 = open("out_numbers2.txt", "w")
    nn(numbers3, numbers2, num2)
    numbers2.close()
    numbers3.close()
    num2.close()
    print("done")
    



if __name__ == "__main__":
    main()
    


    

