report_input = LOAD '/user/cloudera/audi/report_input' USING PigStorage(',')
AS (ad_id: chararray,create_time: chararray,create_date: chararray,create_hour: chararray,log_type: int,imei: chararray,mac_address: chararray,idfa: chararray,app_id: chararray,ip_country: chararray,ip_city: chararray,ip_quadkey: chararray);

set default_parallel 2

grouped = GROUP report_input BY (ad_id, ip_country, ip_city);

count = FOREACH grouped {
    imp_log = FILTER report_input BY log_type == 1;
    click_log = FILTER report_input BY log_type == 2;
    imp_uu = DISTINCT imp_log.imei;
    click_uu = DISTINCT click_log.imei;
    GENERATE FLATTEN(group), COUNT(imp_log), COUNT(imp_uu), COUNT(click_log), COUNT(click_uu);
}

STORE count INTO '/user/cloudera/audi/report_output/ad_geo_location/' USING PigStorage(',');
