
import os

def wordcount(fileName, outputFile):
    dictionary = {}
    punctuation = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    count = 0

    for line in fileName:
        for word in line.split():
            word = word.lower()
            for char in word:
                if char in punctuation:
                    word = word.replace(char, "")
            if word != "":
                if not word in dictionary.keys():
                    dictionary[word] = 1
                else:
                    dictionary[word] += 1

            else:
                count = 0



    

    for i in range(50):
        max_key = max(dictionary, key = dictionary.get)
        outputFile.write(max_key + " " + str(dictionary[max_key]))
        outputFile.write("\n")
        dictionary.pop(max_key)




def main():
    file1 = open(os.path.dirname(__file__) + "/../datasets/cards.txt", 'r')
    file1Out = open("freq_cards.txt", 'w')
    file2 = open(os.path.dirname(__file__) + "/../datasets/article_text.txt", 'r')
    file2Out = open("freq_article_text.txt", 'w')
    file3 = open(os.path.dirname(__file__) + "/../datasets/headlines.txt", 'r')
    file3Out = open("freq_headlines.txt", 'w')
    file4 = open(os.path.dirname(__file__) + "/../datasets/descriptions.txt", 'r')
    file4Out = open("freq_descriptions.txt", 'w')
    wordcount(file1, file1Out)
    wordcount(file2, file2Out)
    wordcount(file3, file3Out)
    wordcount(file4, file4Out)
    print("done")
    file1.close()
    file1Out.close()
    file2.close()
    file2Out.close()
    file3.close()
    file3Out.close()
    file4.close()
    file4Out.close()
    



if __name__ == "__main__":
    main()





