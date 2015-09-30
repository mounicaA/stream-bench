#Query 2:Get the number of days of Data and start and end date for the given specific appliance
#!/bin/sh
start_date=`$HIVE_HOME/bin/hive -e "select to_date(from_unixtime(date2)) as start_date from (select distinct unix_timestamp(Date, 'dd.MM.yy') as date2 from ${hiveconf:tableName}) t2 order by start_date limit 1"`
end_date=`$HIVE_HOME/bin/hive -e "select to_date(from_unixtime(date2)) as end_date from (select distinct unix_timestamp(Date, 'dd.MM.yy') as date2 from ${hiveconf:tableName}) t2 order by end_date DESC limit 1"`
echo "start_date : "  $start_date
echo "end_date : " $end_date
echo "numofdays : "  $(($(($(date -d $end_date "+%s") - $(date -d $start_date "+%s"))) / 86400))