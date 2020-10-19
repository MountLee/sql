log_filter is the log file for filter_data.py, log_ingest is the log file for ingest_data.py, messages in stderr will be write into these two files the records and basic statistics of the processed data (e.g., the number of invalid data rows, and the number of crimes skipped, et al.) would also be write into the stderr of two scripts

If the address/name of the report file is not specifies, then the report will be write into stdout, so one can equivalently generate a report by
>> python.exe generate_report.py > report.txt

# The calling scripts looks like:

python.exe filter_data.py patch-data.csv 2> log_filter | python.exe ingest_data.py 2> log_ingest
python.exe filter_data.py crime-data.csv 2> log_filter | python.exe ingest_data.py 2> log_ingest
python.exe generate_report.py report.txt

# The final scripts would look like:

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