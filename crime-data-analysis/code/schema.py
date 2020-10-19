import csv
import sys
import dateutil.parser
import argparse
import psycopg2

connect = psycopg2.connect(host = 'localhost', database = "postgres", user = "postgres", password = "liwanshan")
cursor = connect.cursor()

cursor.execute("""
	DROP TABLE IF EXISTS crime_blotter;
	CREATE TABLE crime_blotter (
		id INT PRIMARY KEY,
		report_name text DEFAULT 'OFFENSE 2.0' CHECK (report_name in ('ARREST', 'OFFENSE 2.0')),
		section text,
		description text,
		arrest_time timestamp,
		address text,
		neighborhood text,
		zone integer CHECK (zone>=0)
);""")

cursor.execute("""
	DROP TABLE IF EXISTS neighborhoods;
	create table neighborhoods (
	id INT,
	name text PRIMARY KEY,
	location real[]
);""")



cursor.execute("""
	DROP TABLE IF EXISTS crime_types;
	CREATE TABLE crime_types (section text PRIMARY KEY, crime_type text);
	INSERT INTO crime_types (section, crime_type)
	values('3304', 'Criminal mischief'),
	    ('2709', 'Harassment'),
	    ('3502', 'Burglary'),
	    ('13(a)(16)', 'Possession of a controlled substance'),
	    ('13(a)(30)', 'Possession w/ intent to deliver'),
	    ('3701', 'Robbery'),
	    ('3921', 'Theft'),
	    ('3921(a)', 'Theft of movable property'),
	    ('3934', 'Theft from a motor vehicle'),
	    ('3929', 'Retail theft'),
	    ('2701', 'Simple assault'),
	    ('2702', 'Aggravated assault'),
	    ('2501', 'Homicide')
;""")

cursor.execute("""
	ALTER TABLE crime_blotter ADD FOREIGN KEY (neighborhood) REFERENCES neighborhoods;
	ALTER TABLE crime_blotter ADD FOREIGN KEY (section) REFERENCES crime_types;
""")

connect.commit()
# fill data into neighborhood table from police-neighborhoods.csv
cursor = connect.cursor()
with open('police-neighborhoods.csv', 'r',encoding = 'utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    for row in reader:
        nid = int(row[3])
        name = row[2]
        location = [float(row[0]),float(row[1])]
        cursor.execute("INSERT INTO neighborhoods (id, name, location)"
                   "VALUES (%(id)s, %(name)s, %(location)s)",
                   {'id': nid, 'name': name, 'location': location})

connect.commit()