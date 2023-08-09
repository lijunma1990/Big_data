#!/bin/bash


tables=(imp_event_info imp_event_handle s_user imp_equipment_warn imp_equipment_monitor t_wechat_config temp_test)

databaseinput='app_icity'
databaseoutput='ods_icity'
jdbc='jdbc:mysql://172.32.0.72:3306/'${databaseinput}'?serverTimezone=GMT%2B8&useSSL=false&allowPublicKeyRetrieval=true'
password='Ict@123'

for table in ${tables[@]}
	do
		sqoop export \
		--connect ${jdbc} \
		--username root \
		--password ${password} \
		--table ${table} \
		--num-mappers 1 \
		--export-dir /user/hive/warehouse/${databaseoutput}.db/${table} \
		--input-fields-terminated-by "\t" 
	done
