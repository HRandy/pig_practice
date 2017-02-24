-- year_stats.pig
REGISTER '/home/cloudera/workspace/pig101.jar';
DEFINE isGood com.hadoopbook.pig.IsGoodQuality();

records = LOAD '/user/cloudera/pig101/data/1901'
  USING com.hadoopbook.pig.CutLoadFunc('5-10,11-15,16-19,88-92,93-93')
  AS (usaf:chararray, wban:chararray, year:int, temperature:int, quality:int);
  
grouped_records = GROUP records BY year;

year_stats = FOREACH grouped_records {
  uniq_stations = DISTINCT records.usaf;
  good_records = FILTER records BY isGood(quality);
  GENERATE FLATTEN(group), COUNT(uniq_stations) AS station_count,
    COUNT(good_records) AS good_record_count, COUNT(records) AS record_count;
}

DUMP year_stats;

