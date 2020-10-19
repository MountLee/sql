import sys
import csv

spamwriter = csv.writer(sys.stdout, lineterminator='\n')
with open('police-neighborhoods.csv', 'r',newline='',encoding = 'utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    for row in reader:
        # print(row)
       spamwriter.writerow(row)
#        sys.stdout.write(row)

#print('a,b,c\n1,2,3')