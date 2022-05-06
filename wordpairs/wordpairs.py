import boto3
import os
import sys

def wordcount(fileName, outputFile, stopFile):
    dictionary = {}
    punctuation = '!()-[]{};:\'"\,<>./?@#$%^&*_~'
    stopWords = []

    for line in stopFile:
        for word in line.split(','):
            stopWords.append(word)

    for line in fileName:
        words = line.split()
        for i in range(len(words)-1):
            words[i] = words[i].lower()
            for char in words[i]:
                if char in punctuation:
                    words[i] = words[i].replace(char, "")
            words[i+1] = words[i+1].lower()
            for char in words[i+1]:
                if char in punctuation:
                    words[i+1] = words[i+1].replace(char, "")
            
            if i+1 < len(words):
                if not (words[i] == "" or words[i+1] == "" or words[i] == "-" or words[i+1] == "-"): 
                    pair = words[i] + " " + words[i+1]
                    stopW = False
                    for word in stopWords:
                        if word in pair:
                            stopW = True
                    if (stopW == False):
                        if not pair in dictionary.keys():
                            dictionary[pair] = 1
                        else:
                            dictionary[pair] += 1





    for i in range(30):
        max_key = max(dictionary, key = dictionary.get)
        outputFile.write(max_key + " " + str(dictionary[max_key]))
        outputFile.write("\n")
        dictionary.pop(max_key)


def main():
    inputName = sys.argv[1]
    outputName = sys.argv[2]
    stopName = sys.argv[3]
    inputFile = open(inputName, "r")
    outputFile = open(outputName, "w")
    stopFile = open(stopName, "r")
    wordcount(inputFile, outputFile, stopFile)
    inputFile.close()
    outputFile.close()
    stopFile.close()

if __name__ == "__main__":
    main()
