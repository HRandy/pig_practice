-- UDF
REGISTER /home/cloudera/workspace/audi_UDF.jar;
DEFINE generate_location_info com.vpon.wizad.etl.pig.GenerateLocationInfo();

set default_parallel 2

-- load data
raw = LOAD '/user/cloudera/audi/data/*' USING PigStorage(',')
AS (id:chararray, create_time:chararray, action_time:chararray, log_type:int, ad_id:chararray,
positioning_method:int, location_accuracy:double, lat:double, lon:double, cell_id:chararray,
lac:chararray, mcc:chararray, mnc:chararray, ip:chararray, connection_type:int, imei:chararray,
android_id:chararray, udid:chararray, openudid:chararray, idfa:chararray, mac_address:chararray,
uid:chararray, density:double, screen_height:int, screen_width:int, user_agent:chararray,
app_id:chararray, device_model_id:chararray, carrier_id:chararray, os_id:int, os_version:chararray);
-- add location
location_info_added = FOREACH raw GENERATE *, FLATTEN(generate_location_info(ip));

-- store
STORE location_info_added INTO '/user/cloudera/audi/location_info_added/' USING PigStorage(',');
