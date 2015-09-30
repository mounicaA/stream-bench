--Query 4 : Summary of a column - normal mean, median,stddev, min, max
$HIVE_HOME/bin/hive SELECT CAST(MAX(Supply_temperature_primary_flow_temperature) as FLOAT) as maxtemp,CAST(MIN(Supply_temperature_primary_flow_temperature) as FLOAT) as mintemp,CAST(avg(Supply_temperature_primary_flow_temperature) as FLOAT) as mean,CAST(stddev_pop(Supply_temperature_primary_flow_temperature) as FLOAT) as stddeviation,percentile(cast(Supply_temperature_primary_flow_temperature as BIGINT), 0.5) FROM ${hiveconf:tableName} where Supply_temperature_primary_flow_temperature <> '';


