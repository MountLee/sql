from ingest_data import *
import numpy as np
import unittest
import dateutil.parser
import argparse
import psycopg2

def create_table(connect, cursor):
    # Create temporary tables in the database for testing
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

class IngestTest(unittest.TestCase):
    def test_parse_row(self):

        connect = psycopg2.connect(host = 'localhost', database = "postgres", user = "postgres", password = "liwanshan")
        cursor = connect.cursor()
        create_table(connect, cursor)
        report_dict = {'total_num': 0, 'update': 0, 'invalid_data': 0, 'neighbor_invalid': 0, 'neighbor_fixed': 0}
        # report_dict save the statistics used for report

        # normal procedure
        row = ['24784', 'OFFENSE 2.0', '13(a)(16)', 'Possession of Controlled Substance', '2015-03-31T20:54:00', 'N Lexington St & Meade St', 'Point Breeze North', '4']
        row_dict = parse_row(connect, cursor, row, report_dict)
        expect_row_dict = {'id':24784, 'report_name':row[1], 'section':row[2], 'description':row[3],
            'arrest_time':dateutil.parser.parse(row[4]), 'address':row[5], 'neighborhood':'Point Breeze North', 'zone':4}
        self.assertEqual(row_dict, expect_row_dict)
        self.assertEqual(report_dict['invalid_data'], 0)
        self.assertEqual(report_dict['neighbor_invalid'], 0)
        self.assertEqual(report_dict['neighbor_fixed'], 0)

        # invalid ID
        row = ['a', 'OFFENSE 2.0', '9501', 'Bench Warrant', '2015-03-31T20:50:00', '800 block Delmont Ave', 'Beltzhoover', '3']
        row_dict = parse_row(connect, cursor, row, report_dict)
        self.assertEqual(row_dict, {})
        self.assertEqual(report_dict['invalid_data'], 1)
        self.assertEqual(report_dict['neighbor_invalid'], 0)

    def test_adjust_neighbor(self):

        connect = psycopg2.connect(host = 'localhost', database = "postgres", user = "postgres", password = "liwanshan")
        cursor = connect.cursor()
        create_table(connect, cursor)

        report_dict = {'total_num': 0, 'update': 0, 'invalid_data': 0, 'neighbor_invalid': 0, 'neighbor_fixed': 0}

        # invalid neighborhood name
        row = ['24828', 'ARREST', '2701', 'Simple Assault', '2015-03-31T20:53:00', '5400 block Hays St', 'AAA BBB', '5']
        neighborhood = adjust_neighbor(connect, cursor, row[6], row, report_dict)
        self.assertEqual(neighborhood, [])
        self.assertEqual(report_dict['neighbor_invalid'], 1)
        self.assertEqual(report_dict['neighbor_fixed'], 0)

        # fix a neighborhood name
        row = ['24828', 'ARREST', '2701', 'Simple Assault', '2015-03-31T20:53:00', '5400 block Hays St', 'EaSA liberTY', '5']
        neighborhood = adjust_neighbor(connect, cursor, row[6], row, report_dict)
        self.assertEqual(neighborhood, [('East Liberty',)])
        self.assertEqual(report_dict['neighbor_invalid'], 1)
        self.assertEqual(report_dict['neighbor_fixed'], 1)        

        # fix a neighborhood name
        row = ['24828', 'ARREST', '2701', 'Simple Assault', '2015-03-31T20:53:00', '5400 block Hays St', 'EaST liberTY haha', '5']
        neighborhood = adjust_neighbor(connect, cursor, row[6], row, report_dict)
        self.assertEqual(neighborhood, [('East Liberty',)])
        self.assertEqual(report_dict['neighbor_invalid'], 1)
        self.assertEqual(report_dict['neighbor_fixed'], 2)   

        # fix a neighborhood name
        row = ['24828', 'ARREST', '2701', 'Simple Assault', '2015-03-31T20:53:00', '5400 block Hays St', 'EaST liber', '5']
        neighborhood = adjust_neighbor(connect, cursor, row[6], row, report_dict)
        self.assertEqual(neighborhood, [('East Liberty',)])
        self.assertEqual(report_dict['neighbor_invalid'], 1)
        self.assertEqual(report_dict['neighbor_fixed'], 3)

    def test_insert_blotter(self):

        connect = psycopg2.connect(host = 'localhost', database = "postgres", user = "postgres", password = "liwanshan")
        cursor = connect.cursor()
        create_table(connect, cursor)

        report_dict = {'total_num': 0, 'update': 0, 'invalid_data': 0, 'neighbor_invalid': 0, 'neighbor_fixed': 0}
        
        # normal procedure
        row = ['24784', 'OFFENSE 2.0', '13(a)(16)', 'Possession of Controlled Substance', '2015-03-31T20:54:00', 'N Lexington St & Meade St', 'Point Breeze North', '4']
        row_dict = {'id':int(row[0]), 'report_name':row[1], 'section':row[2], 'description':row[3],
            'arrest_time':dateutil.parser.parse(row[4]), 'address':row[5], 'neighborhood':'Point Breeze North', 'zone':4}

        insert_blotter(connect, cursor, row_dict, report_dict)
        self.assertEqual(report_dict['total_num'], 1)

        # duplicated ID
        row = ['24784', 'OFFENSE 2.0', '13(a)(16)', 'Possession of Controlled Substance', '2015-03-31T20:54:00', 'N Lexington St & Meade St', 'Point Breeze North', '4']
        row_dict = {'id':int(row[0]), 'report_name':row[1], 'section':row[2], 'description':row[3],
            'arrest_time':dateutil.parser.parse(row[4]), 'address':row[5], 'neighborhood':'Point Breeze North', 'zone':4}

        insert_blotter(connect, cursor, row_dict, report_dict)
        self.assertEqual(report_dict['total_num'], 1)
        self.assertEqual(report_dict['update'], 1)

        # invalid data
        row = ['24785', 'OFFENSE 2.0', '13(a)(16)', 'Possession of Controlled Substance', '2015-03-31T20:54:00', 'N Lexington St & Meade St', 'Haha', '4']
        row_dict = {'id':int(row[0]), 'report_name':row[1], 'section':row[2], 'description':row[3],
            'arrest_time':dateutil.parser.parse(row[4]), 'address':row[5], 'neighborhood':'Haha', 'zone':4}

        insert_blotter(connect, cursor, row_dict, report_dict)
        self.assertEqual(report_dict['total_num'], 1)
        self.assertEqual(report_dict['invalid_data'], 1)        

########################## command-line driver for unittest ###########################

if __name__ == "__main__":
    unittest.main()