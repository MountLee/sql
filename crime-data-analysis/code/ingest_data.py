################################################################
# The second script should read this CSV from STDIN and load the 
# incidents into the table you defined in schema.sql.
# It can be used for both raw crime blotter files and patch files
# for patch files, every row that substitute the old data will be reported
################################################################

import csv
import sys
import dateutil.parser
import argparse
import psycopg2

def ingest(filename, connect, cursor):
	"""Ingest the data given by filename

	------------------------------------
	Key arguments:

	connect, cursor -- variables connecting the database
	filename        -- address of the file, if filename == '' then read the data from stdin 
	"""
	report_dict = {'total_num': 0, 'update': 0, 'invalid_data': 0, 'neighbor_invalid': 0, 'neighbor_fixed': 0}
	# report_dict save the statistics used for report
	if filename:
		file = open(filename, 'r', encoding = 'utf-8')
	else:
		file = sys.stdin
	reader = csv.reader(file, delimiter=',')
	next(reader)
	for row in reader:
		row_dict = parse_row(connect, cursor, row, report_dict)
		if not row_dict:
			# row has some problems and cannot be used
			continue
		else:
			insert_blotter(connect, cursor, row_dict, report_dict)
	file.close()
	write_report(report_dict)

def parse_row(connect, cursor, row_list, report_dict):
	""" construct a dictionary from row_list used for inserting into database """
	# text data
	report_name = row_list[1]
	section = row_list[2]
	description = row_list[3]
	address = row_list[5]
	neighborhood = row_list[6]
	# integers and timestamps, if the values cannot be converted, then raise error to stderr
	# arrest_time = dateutil.parser.parse(row_list[4])
	# report_id = int(row_list[0])
	# zone = int(row_list[7])	
	try:
		arrest_time = dateutil.parser.parse(row_list[4])
		report_id = int(row_list[0])
		zone = int(row_list[7])
	except:
		# data type is invalid
		sys.stderr.write('Invalid key values (ID, zone, or time). Row: \n' + '    ' + ' '.join(row_list)+'\n')
		report_dict['invalid_data'] += 1
		return {}
	neighborhood = adjust_neighbor(connect, cursor, neighborhood, row_list, report_dict)
	# try to find standard neighborhood name matching the name in the row
	if not neighborhood:
		# cannot match neighborhood name
		return {}
	else:
		row_dict = {'id':report_id, 'report_name':report_name, 'section':section, 'description':description,
					'arrest_time':arrest_time, 'address':address, 'neighborhood':neighborhood[0][0], 'zone':zone}
		return row_dict

def insert_blotter(connect, cursor, row_dict, report_dict):
	""" insert one row from the input data to table "crime_blotter" in the database 

	------------------------------------------
	Key arguments:

	connect, cursor -- variables connecting the database
	row_dict        -- dictionary of elements in the row
	report_dict     -- dict containing some numbers for report
	"""
	try:
		cursor.execute("INSERT INTO crime_blotter (id, report_name, section, description, arrest_time, address, neighborhood, zone)"
					"VALUES (%(id)s, %(report_name)s, %(section)s, %(description)s, %(arrest_time)s, %(address)s, %(neighborhood)s, %(zone)s)",
					row_dict)
		report_dict['total_num'] += 1
	except psycopg2.IntegrityError as ex:
		connect.reset()    # reset the connection
		cursor = connect.cursor()
		row = [str(row_dict['id']), row_dict['report_name'], row_dict['section'], row_dict['description'],
			str(row_dict['arrest_time']), row_dict['address'], row_dict['neighborhood'], str(row_dict['zone'])]
		if ex.pgcode == '23505':
			# the ID of the current row is duplicated
			sys.stderr.write('Duplicated ID occurs. Data: \n')
			sys.stderr.write('    ' + ' '.join(row) + '\n')
			# update the database based on the current row 
			# for patch data this is necessary, for new blotter this is also acceptable
			# since duplicated rows in new blotter should be equivalent
			# cursor.execute("UPDATE crime_blotter SET id = %(id)s, name = %(name)s, section = %(section)s, description = %(description)s, "
			# 		"arrest_time = %(arrest_time)s, address = %(address)s, neighborhood = %(neighborhood)s, zone = %(zone)s"
			# 		"WHERE id = %(cur_id)s", row_dict.update({'cur_id': row_dict['id']}))
			cursor.execute("UPDATE crime_blotter SET id = %s, report_name = %s, section = %s, description = %s, "
			"arrest_time = %s, address = %s, neighborhood = %s, zone = %s"
			"WHERE id = %s", [row_dict['id'], row_dict['report_name'], row_dict['section'], row_dict['description'],
			row_dict['arrest_time'], row_dict['address'], row_dict['neighborhood'], row_dict['zone'], row_dict['id']])
			report_dict['update'] += 1
		elif ex.pgcode == '23503' or ex.pgcode == '42830':
			sys.stderr.write('Foreign key violation occurs. Data: \n')
			sys.stderr.write('    ' + ' '.join(row) + '\n')
			report_dict['invalid_data'] += 1
		else:
			sys.stderr.write('Some violation in database occurs. Data: \n')
			sys.stderr.write('    ' + ' '.join(row) + '\n')
			report_dict['invalid_data'] += 1
	connect.commit()

def adjust_neighbor(connect, cursor, neigh_name, row, report_dict):
	"""Adjust the name of neighbohoods in the crime blotter data

	-----------------------
	Key arguments:

	connect, cursor -- variables connecting the database
	neigh_name      -- name of the neighborhood in the current data row
	row             -- list of the current data row
	report_dict     -- dict containing some numbers for report
	-----------------------
	Return:
	neighborhood    -- the standard neighborhood best matching the given neigh_name,
					   if nothing can match neigh_name, report to stderr
					   if not exactly matching, report to stderr 
	"""
	# notice that in 'WHERE' condition, we should use LOWER() to convert all letters to lower case to compare
	# first find name that exactly match neigh_name
	cursor.execute("""SELECT name FROM neighborhoods
						WHERE LOWER(name) = %s""", (neigh_name.lower(),))
	neighborhood = cursor.fetchall()
	if neighborhood:
		return neighborhood
	# otherwise, use approximate matching to find best matching name
	if not neighborhood:
		# nothing was selected
		cursor.execute("""SELECT name FROM neighborhoods
							WHERE LOWER(name) LIKE %s""", ('%' + neigh_name.lower() + '%',))
		neighborhood = cursor.fetchall()
	if not neighborhood:
		# nothing was selected
		cursor.execute("""SELECT name FROM neighborhoods
							WHERE %s LIKE '%%' || LOWER(name) || '%%'""", (neigh_name.lower(),))
		neighborhood = cursor.fetchall()
	if not neighborhood:
		# nothing was selected
		cursor.execute("""SELECT name from neighborhoods
							WHERE levenshtein(LOWER(%s), LOWER(name)) < 5
							ORDER BY levenshtein(LOWER(%s), LOWER(name))""", (neigh_name, neigh_name))
		neighborhood = cursor.fetchall()
		# fetchone will fetch the first record of the result of "SELECT", here the record with the smallest distance
	if not neighborhood:
		# we cannot find any name that is similar to neigh_name
		sys.stderr.write('Invalid neighborhood name. Data: \n')
		sys.stderr.write('    ' + ' '.join(row) + '\n')
		report_dict['neighbor_invalid'] += 1
	else:
		sys.stderr.write('Best matched neighborhood name: ' + neighborhood[0][0] + '. Data: \n')
		sys.stderr.write('    ' + ' '.join(row) + '\n')
		report_dict['neighbor_fixed'] += 1
	return neighborhood

def write_report(report_dict):
	# Format of report_dict : 
	# {'total_num': 0, 'updata': 0, 'invalid_data': 0, 'neighbor_invalid': 0, 'neighbor_fixed': 0}
	for name in report_dict.keys():
		sys.stderr.write(name + ': ' + str(report_dict[name]) + '\n')

def get_database(file_name):
	if not file_name:
		# use the default database
		database = {'host':'localhost', 'database':"postgres", 'user':"postgres", 'password':"liwanshan"}
	else:
		# use the user-provided database
		with open(file_name, 'r', encoding = 'utf-8') as file:
			row = file.readline().strip()
			host = row[6:].strip()
			
			row = file.readline().strip()
			base = row[10:].strip()
			
			row = file.readline().strip()
			user = row[6:].strip()
			
			row = file.readline().strip()
			password = row[10:].strip()		

		database = {'host':host, 'database':base, 'user':user, 'password':password}
	return database

############################## command line driver ##################################

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Filter the blotter weekly record.')

	parser.add_argument('--file', metavar='address', type = str, default = '',
	                    help='Address of the record file.')
	parser.add_argument('--database', metavar='db_connect', type = str, default = '',
	                    help='Address of the file containing the connection information to the database.')
	args = parser.parse_args()

	database = get_database(args.database)
	connect = psycopg2.connect(host = database['host'], database = database['database'], user = database['user'], password = database['password'])
	cursor = connect.cursor()
	# fill data into blotter table from args.file

	ingest(args.file, connect, cursor)


"""
the file saving the information for database must be in the following format:

host: your_host_name
database: your_database_name
user: your_user_name
password: your_password
"""

"""
when calling with user-provided database, just run:
ingest_data.py --database=your_file
"""