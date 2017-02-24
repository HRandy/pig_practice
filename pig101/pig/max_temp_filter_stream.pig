-- max_temp_filter_stream.pig
DEFINE is_good_quality `is_good_quality.py`
  SHIP ('/home/cloudera/Desktop/pig101/python/is_good_quality.py');

records = LOAD '/user/cloudera/pig101/data/sample.txt' USING PigStorage('\t')
  AS (year:chararray, temperature:int, quality:int);

filtered_records = STREAM records THROUGH is_good_quality
  AS (year:chararray, temperature:int);

grouped_records = GROUP filtered_records BY year;

max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);

DUMP max_temp;