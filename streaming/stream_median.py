import boto3
import sys
import random
import statistics

def median(inputObjName, outFileName, numSamples):
    s3 = boto3.resource('s3')
    obj = s3.Object("326-data-bucket", inputObjName)
    obj_dict = obj.get()
    b_stream = obj_dict["Body"]
    it = b_stream.iter_lines(chunk_size = 1024)

    cols = []
    filelen = 0
    samples = 0
    additions = []
    for i in range(116):
        additions.append(0)

    for line in it:
        filelen += 1
        line = line.decode()
        currentcol = line.split(',')
        for i in range(len(currentcol)):
            if len(cols) < 117:
                cols.append([float(currentcol[i])])
            elif len(cols[i]) < numSamples:
                cols[i].append(float(currentcol[i]))
            else:
                probability = numSamples + additions[i]
                if random.randint(1, probability) == probability:
                    cols[i][random.randint(0,99)] = float(currentcol[i])
                additions[i] += 1
    medians = []
    for col in cols:
        #print(col)
        medians.append(statistics.median(col))
    #print(additions)

    newFile = open(outFileName, "w")
    newFile.write("List of Medians" + "\n")
    for i in range(len(medians)):
        newFile.write("Column " + str(i) + " median: " + str(medians[i]) + "\n")

def main():
    median(str(sys.argv[1]), str(sys.argv[2]), int(sys.argv[3]))

if __name__ == "__main__":
    main()
