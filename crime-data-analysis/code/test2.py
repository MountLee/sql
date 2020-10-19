import sys
import csv

# data = sys.stdin.readlines()
# csvreader = csv.reader(data, delimiter=',')

spamwriter = csv.writer(sys.stdout, lineterminator='\n')
csvreader = csv.reader(sys.stdin, delimiter=',')
for row in csvreader:
    # print(','.join(row))
    #if len(row)>1:
    spamwriter.writerow(row)

# print('The second file.')