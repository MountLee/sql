python schema.py

python.exe filter_data.py crime-base.csv 2> log_filter_base | python.exe ingest_data.py 2> log_ingest_base

python.exe filter_data.py crime-week-1-patch.csv 2> log_filter_patch_1 | python.exe ingest_data.py 2> log_ingest_patch_1
python.exe filter_data.py crime-week-1.csv 2> log_filter_week_1 | python.exe ingest_data.py 2> log_ingest_week_1

python.exe filter_data.py crime-week-2-patch.csv 2> log_filter_patch_2 | python.exe ingest_data.py 2> log_ingest_patch_2
python.exe filter_data.py crime-week-2.csv 2> log_filter_week_2 | python.exe ingest_data.py 2> log_ingest_week_2

python.exe filter_data.py crime-week-3-patch.csv 2> log_filter_patch_3 | python.exe ingest_data.py 2> log_ingest_patch_3
python.exe filter_data.py crime-week-3.csv 2> log_filter_week_3 | python.exe ingest_data.py 2> log_ingest_week_3

python.exe filter_data.py crime-week-4-patch.csv 2> log_filter_patch_4 | python.exe ingest_data.py 2> log_ingest_patch_4
python.exe filter_data.py crime-week-4.csv 2> log_filter_week_4 | python.exe ingest_data.py 2> log_ingest_week_4

python.exe generate_report.py report.txt 2> log_report