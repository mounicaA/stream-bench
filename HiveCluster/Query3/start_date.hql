--Query3: Display the error and specific start and end date of the error, and the display_code
select current_fault_display_code, Date, Time, Operating_status_Error_locking from ${hiveconf:tableName} where Operating_status_Error_locking = '1'