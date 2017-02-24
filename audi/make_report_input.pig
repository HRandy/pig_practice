location_info_added = LOAD '/user/cloudera/audi/location_info_added' USING PigStorage(',')
AS (id:chararray, create_time:chararray, action_time:chararray, log_type:int, ad_id:chararray,
positioning_method:int, location_accuracy:double, lat:double, lon:double, cell_id:chararray,
lac:chararray, mcc:chararray, mnc:chararray, ip:chararray, connection_type:int, imei:chararray,
android_id:chararray, udid:chararray, openudid:chararray, idfa:chararray, mac_address:chararray,
uid:chararray, density:double, screen_height:int, screen_width:int, user_agent:chararray,
app_id:chararray, device_model_id:chararray, carrier_id:chararray, os_id:int, os_version:chararray,
ip_country:chararray, ip_city:chararray, ip_lat:chararray, ip_lon:chararray, ip_quadkey:chararray);


-- cleaned data
report_input = FOREACH location_info_added GENERATE ad_id, create_time, SUBSTRING(create_time,0,10) AS create_date,
               SUBSTRING(create_time,11,13) AS create_hour, log_type, imei, mac_address, idfa, app_id,
               ip_country, ip_city, ip_quadkey;

STORE report_input INTO '/user/cloudera/audi/report_input' USING PigStorage(',');


