import sys
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import dateutil.parser
import argparse
import tabulate
import psycopg2

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

parser = argparse.ArgumentParser(description='Filter the blotter weekly record.')

# report will be write into the args.file, if args.file is not identified, then the report will be
# write into stdout
parser.add_argument('file', metavar='address', type = str, nargs = '?', default = '',
                    help='Address/name of the report file.')
parser.add_argument('--database', metavar='db_connect', type = str, default = '',
                    help='Address of the file containing the connection information to the database.')
args = parser.parse_args()

database = get_database(args.database)
connect = psycopg2.connect(host = database['host'], database = database['database'], user = database['user'], password = database['password'])
cursor = connect.cursor()

if not args.file:
    report_file = sys.stdout
else:
    report_file = open(args.file, 'w')

###############################
# weekly count of crimes
###############################

cursor.execute("""SELECT EXTRACT(week FROM crime_blotter.arrest_time) as week, crime_types.crime_type as crime_type,  COUNT(crime_blotter.id) as crime_num
                FROM crime_blotter LEFT OUTER JOIN crime_types 
                ON crime_blotter.section = crime_types.section
                GROUP BY week, crime_type
                ORDER BY week;""")

# print a table
table = cursor.fetchall()
headers = ['week', 'crime_type', 'crime_num']
report_file.write('\n#Weekly count of crimes\n\n')
report_file.write(tabulate.tabulate(table, headers, tablefmt = 'presto'))
report_file.write('\n')

###############################
# daily count of crimes
###############################

cursor.execute("""SELECT DATE(arrest_time) as date, COUNT(id) as crime_num
                FROM crime_blotter
                WHERE DATE(arrest_time) >= (SELECT MAX(DATE(arrest_time)) FROM crime_blotter) - 30
                GROUP BY date
                ORDER BY date DESC;""")

# print a table
table = cursor.fetchall()
headers = ['date', 'crime_num']
report_file.write('\n#Daily count of crimes\n\n')
report_file.write(tabulate.tabulate(table, headers, tablefmt = 'presto'))
report_file.write('\n')

###############################
# draw the curve
###############################

date = []
crime_num = []
for row in table:
    date.append(row[0])
    crime_num.append(row[1])   
crime_num = np.array(crime_num)

plt.plot_date(date, crime_num, '-')
_ = plt.setp(plt.gca().xaxis.get_majorticklabels(), 'rotation', 90)
# The name of the graph is defaultly set as 'Daily_crime' + str(date[0]) + '-' + str(date[-1]) + '.png'
plt.savefig('Daily_crime' + str(date[0]) + '-' + str(date[-1]) + '.png', bbox_inches = 'tight')
#plt.savefig('Daily_crime.png', bbox_inches = 'tight')

###############################
# weekly change on neighborhood
###############################

cursor.execute("""SELECT this_week.neighborhood, coalesce(this_week.crime_num, 0) as crime_this_week, 
    coalesce(last_week.crime_num, 0) as crime_last_week, 
    (coalesce(this_week.crime_num, 0) - coalesce(last_week.crime_num, 0)) as change
    FROM (SELECT count(crime_blotter) as crime_num, neighborhood
        FROM crime_blotter
        WHERE EXTRACT(week from arrest_time) = (SELECT MAX(EXTRACT(week from arrest_time)) FROM crime_blotter)
        GROUP BY neighborhood) this_week
    LEFT OUTER JOIN (SELECT  COUNT(crime_blotter) as crime_num, neighborhood
        FROM crime_blotter
        WHERE EXTRACT(week from arrest_time) = (SELECT MAX(EXTRACT(week from arrest_time)) FROM crime_blotter) - 1
        GROUP BY neighborhood) last_week
    ON this_week.neighborhood = last_week.neighborhood;""")

# print a table
table = cursor.fetchall()
headers = ['neighborhood', 'crime_this_week', 'crime_last_week', 'change']
report_file.write('\n#weekly change on neighborhood\n\n')
report_file.write(tabulate.tabulate(table, headers, tablefmt = 'presto'))
report_file.write('\n')

##############################
# weekly change on police zone
##############################

cursor.execute("""SELECT this_week.zone, coalesce(this_week.crime_num, 0) as crime_this_week, 
    coalesce(last_week.crime_num, 0) as crime_last_week, 
    (coalesce(this_week.crime_num, 0) - coalesce(last_week.crime_num, 0)) as change
    FROM (SELECT COUNT(crime_blotter) as crime_num, zone
        FROM crime_blotter
        WHERE EXTRACT(week from arrest_time) = (SELECT MAX(EXTRACT(week from arrest_time)) FROM crime_blotter)
        GROUP BY zone) this_week
    LEFT OUTER JOIN (SELECT  COUNT(crime_blotter) as crime_num, zone
        FROM crime_blotter
        WHERE EXTRACT(week from arrest_time) = (SELECT MAX(EXTRACT(week from arrest_time)) FROM crime_blotter) - 1
        GROUP BY zone) last_week
    ON this_week.zone = last_week.zone;""")

# print a table
table = cursor.fetchall()
headers = ['police_zone', 'crime_this_week', 'crime_last_week', 'change']
report_file.write('\n#weekly change on police zone\n\n')
report_file.write(tabulate.tabulate(table, headers, tablefmt = 'presto'))
report_file.write('\n')

report_file.close()