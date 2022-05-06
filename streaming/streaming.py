import boto3

def streamSummary(inputFileName, outputFileName):
    s3 = boto3.resource('s3')
    obj = s3.Object("326-data-bucket", inputFileName)
    obj_dict = obj.get()
    b_stream = obj_dict["Body"]

    it = b_stream.iter_lines(chunk_size = 1024)

    file_len = 0
    averagecol = []
    maxcol = []
    mincol = []
    nonzerocols = []

    for line in it:
        file_len += 1
        line = line.decode()
        currentcol = line.split(',')
        for i in range(len(currentcol)):
            if len(averagecol) < 117:
                averagecol.append(float(currentcol[i]))
            else: 
                averagecol[i] += float(currentcol[i])
            if len(maxcol) < 117:
                maxcol.append(float(currentcol[i]))
            else:
                if maxcol[i] < float(currentcol[i]):
                    maxcol[i] = float(currentcol[i])
            if len(mincol) < 117:
                mincol.append(float(currentcol[i]))
            else: 
                if mincol[i] > float(currentcol[i]):
                    mincol[i] = float(currentcol[i])
            if len(nonzerocols) < 117:
                if float(currentcol[i]) != 0:
                    nonzerocols.append(1)
                else:
                    nonzerocols.append(0)
            else:
                if float(currentcol[i]) != 0:
                    nonzerocols[i] += 1

    newfile = open(outputFileName, "w")
    newfile.write("Number of Lines: "+"\n")
    newfile.write(str(file_len)+"\n")
    newfile.write("AVERAGE: "+"\n")

    for i in averagecol:
        i /= file_len
        newfile.write(str(i) + ",")
    newfile.write("\n")
    newfile.write("MAXIMUM: "+"\n")

    for i in maxcol:
        newfile.write(str(i) + ",")
    newfile.write("\n")
    newfile.write("MINIMUM: "+"\n")

    for i in mincol:
        newfile.write(str(i) + ",")
    newfile.write("\n")
    newfile.write("NON ZERO ENTRIES: "+"\n")
 
    for i in nonzerocols:
        newfile.write(str(i) + ",")
    newfile.write("\n")
    newfile.close()

    b_stream.close()

def main():
    streamSummary("Scan_dataset.csv", "scan_summary.txt")

if __name__ == "__main__":
    main()
