#Script to create a table in Hive and load data from the given path into it
#Usage: python toHive.py <table name> <table path>

#Requirements: 
#1. pyhive and pyhs2 should be installed. The links are given in the readme file.
#2. The required csv file should be on hdfs. The table path is the path of the file on hdfs

import pyhs2
import pyhive

conn = pyhs2.connect(host='localhost',
                   port=10000,
                   authMechanism="PLAIN",
                   user='root',
                   password='test',
                   database='boschData')
cur = conn.cursor()

tableName = sys.argv[1]
#hdfs path for the file
tablePath = sys.argv[2]


query = """Create external table """+tableName+""" bracketopen
Date STRING,
Time STRING,
System_time STRING,
ECU-Type STRING, 
ECU_SW-Version STRING, 
ECU_Identification STRING, 
Code_plug_number STRING, 
Nominal_(maximum)_burner_power STRING, 
Minimal_Burner_load STRING, 
Hot_water_system STRING, 
Power_setpoint STRING, 
Actual_Power STRING, 
Current_fault_display_code STRING, 
Current_fault_cause_code STRING, 
Number_of_burner_starts STRING, 
Working_time_total_system STRING, 
Working_time_total_of_the_burner STRING, 
Working_time_central_heating STRING, 
Working_time_DHW STRING, 
ClipIn_FW_Type STRING, 
ClipIn_Bosch_FW_mainversion STRING, 
ClipIn_Bosch_FW_subversion STRING, 
Operating_status_Central_heating_active STRING, 
Operating_status_Hot_water_active STRING, 
Operating_status_Chimmney_sweeper_active STRING, 
Operating_status_Flame STRING, 
Operating_status_Heatup_phase STRING, 
Operating_status_Error_Locking STRING, 
Operating_status_Error_Blocking STRING, 
Operating_status_Maintenance_request STRING, 
Anti_fast_cycle_time_DT25 STRING, 
PWM_pump_present STRING, 
Fluegas_sensor_present STRING, 
Pressure_sensor_present STRING, 
Return_sensor_present STRING, 
Relay_status_Gasvalve STRING, 
Relay_status_Fan STRING, 
Relay_status_Ignition STRING, 
Relay_status_CH_pump STRING, 
Relay_status_internal_3-way-valve STRING, 
Relay_status_HW_circulation_pump STRING, 
External_cut_off_switch STRING, 
Safety_Temperature_Limiter_MAX STRING, 
RTH_switch_(external_onoff_control) STRING, 
Outdoor_temperature STRING, 
Number_of_starts_central_heating STRING, 
Supply_temperature_(primary_flow_temperature)_setpoint STRING, 
Supply_temperature_(primary_flow_temperature) STRING, 
CH_pump_modulation STRING, 
Low_loss_header_temperature STRING, 
(Primary)_Return_temperature STRING, 
Central_heating_blocked STRING, 
Floor_drying_mode_active STRING, 
System_water_pressure STRING, 
Programmer_channel_for_central_heating_active STRING, 
Central_heating_switch_onoff STRING, 
Maximum_supply_(primary_flow)_temperature STRING, 
Maximum_central_heating_power STRING, 
Supply_temp__Pos._tolerance STRING, 
Supply_temp__Neg._tolerance STRING, 
Anti_fast_cycle_time_DT22 STRING, 
Pump_functionality_switch STRING, 
Pump_post_purge_time STRING, 
Pump_head_selection STRING, 
Heat_request_status_Central_heating_via_EMS-bus STRING, 
Heat_request_status_Central_heating_via_Switch STRING, 
Heat_request_status_Central_heating_Frost STRING, 
Heat_request_status_Hot_water_Frost STRING, 
Heat_request_status_Hot_water_detection_internal STRING, 
Heat_request_status_Hot_water_detection_via_EMS-bus STRING, 
Number_of_starts_hot_water STRING, 
Hot_water_temperature_setpoint STRING, 
Hot_water_outlet_temperature STRING, 
Hot_water_storage_temperature STRING, 
Hot_water_flow_sensor_(turbine) STRING, 
Programmer_channel_for_hot_water_active STRING, 
Hot_water_installed_at_appliance STRING, 
Hot_water_switch STRING, 
Hot_water_supply_temperature_offset STRING, 
Hot_water_circulation_pump STRING, 
Circulation_pump_starts_per_Hour STRING, 
Thermal_disinfection_setpoint STRING, 
Diverter_valve_or_Chargepump STRING, 
DHW_priority STRING, 
Hot_water_day_function STRING, 
Hot_water_one_time_loading STRING, 
Hot_water_thermal_disinfection STRING, 
Hot_water_system_is_being_heated STRING, 
Hot_water_system_is_being_post_heated STRING, 
Hot_water_setpoint_is_reached STRING, 
Hot_water_priority_status STRING, 
Error_status_byte_DHW_Hot_water_sensor_1_is_defect STRING, 
Error_status_byte_DHW_Thermal_Disinfection_did_not_work STRING, 
Service_Request_Setting STRING, 
Service_after_burner_operating_time STRING, 
Service_after_date_Day STRING, 
Service_after_date_Month STRING, 
Service_after_date_Year STRING, 
Service_after_appliance_operating_time STRING, 
Voltage_measured_1-2-4_connection STRING, 
Temperature_combustion_chamber STRING, 
Used_fan_map STRING, 
Actual_flow_rate_turb:ine STRING, 
External_frost_thermostat_230Vac STRING, 
On_Off_HW_demand_230Vac STRING, 
On_Off_room_thermostat_230Vac_is_Y_S_compliant STRING, 
Flame_current STRING, 
Fan_speed STRING, 
Fan_speed_setpoint STRING bracketclose
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY 'tabcharacter'
	LINES TERMINATED BY 'newline'
    tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="2")
STORED AS INPUTFORMAT
    'org:apache:hadoop:mapred:TextInputFormat'
OUTPUTFORMAT
	'org:apache:hadoop:hive:ql:io:HiveIgnoreKeyTextOutputFormat'
    tblproperties ("skip.header.line.count"="1")"""

query = query.replace("\n", " ")
query = query.replace("\t", " ")
query = query.replace("tabcharacter", "\t")
query = query.replace("newline", "\n")
query = query.replace("-", "_")
query = query.replace("(", "")
query = query.replace(")", "")
query = query.replace("bracketopen", "(")
query = query.replace("bracketclose", ")")
query = query.replace(".", "")
query = query.replace(":", ".")

cur.execute(query)
print("table % s created!" % tableName)

loadDataQuery = """load data inpath '"""+tablePath+"""' into table """+tableName
cur.execute(loadDataQuery)
print("data loaded for table % s" % tableName)

