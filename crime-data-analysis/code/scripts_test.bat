python schema.py

python.exe filter_data.py crime-week-1.csv 2> log_filter_week_1 | python.exe ingest_data.py --database=db_infor.txt 2> log_ingest_week_1