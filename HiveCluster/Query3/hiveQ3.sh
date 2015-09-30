#Query3:Running query 3 on all appliances
#!/bin/sh
for line in $(cat hivetables.txt) ; 
do
     time_results=$($HIVE_HOME/bin/hive -hiveconf tableName=$line -f start_date.hql)  	 
done