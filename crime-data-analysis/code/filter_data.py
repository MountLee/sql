# The first script should accept the name of the blotter CSV file as a command-line 
# argument and filter the data, only including OFFENSE 2.0 reports of the types indicated 
# in Table 1, and skipping crimes with no zone. It should print the result as a CSV file 
# to STDOUT.
import csv
import sys
import dateutil.parser
import argparse

def filter(filename, offense):
	# the filename should contain header
	stdout_writer = csv.writer(sys.stdout, lineterminator='\n')
	with open(filename, 'r', encoding = 'utf-8') as csvfile:
		number_missing = 0
		reader = csv.reader(csvfile)
		header = next(reader)
		stdout_writer.writerow(header)
		for row in reader:
			report_id = row[0]
			if row[1] == '':
				row[1] = 'OFFENSE 2.0'
			report_name = row[1]
			section = row[2]
			description = row[3]
			arrest_time = row[4]
			address = row[5]
			neighborhood = row[6]
			zone = row[7]
			if description == '' or arrest_time == '' or address == '' or neighborhood == '' or zone == '':
				number_missing += 1
				sys.stderr.write('Missing key values. Row: \n' + '    ' + ' '.join(row)+'\n')
				continue
			if report_name != 'OFFENSE 2.0' or section not in offense:
				continue
			stdout_writer.writerow(row)
		sys.stderr.write('Number of rows containing missing key values: '+str(number_missing)+'\n')

############################## command line driver ##################################
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Filter the blotter weekly record.')

	parser.add_argument('file', metavar='address', type = str, nargs = 1,
	                    help='Address of the record file.')

	# Now run the main program
	args = parser.parse_args()
	offense = ['3304','2709','3502','13(a)(16)','13(a)(30)','3701','3921','3921(a)','3934','3929','2701','2702','2501']
	filter(args.file[0], offense)