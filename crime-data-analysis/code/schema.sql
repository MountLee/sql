DROP TABLE IF EXISTS neighborhoods;
create table neighborhoods (
	id INT PRIMARY KEY,
	name text,
	location real[]
);

DROP TABLE IF EXISTS crime_blotter;
create table crime_blotter (
	id INT PRIMARY KEY,
	report_name, text DEFAULT 'OFFENSE 2.0' CHECK (state in ('ARREST', 'OFFENSE 2.0')),
	section text,
	discription text,
	arrest_time timestamp,
	address, text,
	neighborhood text,
	zone integer CHECK (zone>=0)
);

DROP TABLE IF EXISTS crime_types;
CREATE TABLE crime_types (section text PRIMARY KEY, crime_type text)
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
    ('2501', 'Homicide');

ALTER TABLE crime_blotter ADD FOREIGN KEY (neighborhood) REFERENCES neighborhoods;
ALTER TABLE crime_blotter ADD FOREIGN KEY (section) REFERENCES crime_types;
