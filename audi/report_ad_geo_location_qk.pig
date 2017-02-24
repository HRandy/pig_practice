REGISTER /home/cloudera/workspace/audi_UDF.jar;
DEFINE get_geo_locations com.vpon.wizad.etl.pig.GetGeoLocations('ALL');

report_input = LOAD '/user/cloudera/audi/report_input' USING PigStorage(',')
AS (ad_id: chararray,create_time: chararray,create_date: chararray,create_hour: chararray,log_type: int,imei: chararray,mac_address: chararray,idfa: chararray,app_id: chararray,ip_country: chararray,ip_city: chararray,ip_quadkey: chararray);

set default_parallel 1
set mapred.compress.map.output true

region_added = FOREACH report_input GENERATE ad_id, log_type, imei, FLATTEN(get_geo_locations(ip_quadkey));
grouped = GROUP region_added BY (ad_id, region);

count = FOREACH grouped {
    imp_log = FILTER region_added BY log_type == 1;
    click_log = FILTER region_added BY log_type == 2;
    imp_uu = DISTINCT imp_log.imei;
    click_uu = DISTINCT click_log.imei;
    GENERATE FLATTEN(group), COUNT(imp_log), COUNT(imp_uu), COUNT(click_log), COUNT(click_uu);
}

STORE count INTO '/user/cloudera/audi/report_output/ad_geo_location_qk/' USING PigStorage(',');
