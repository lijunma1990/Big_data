#!/bin/bash


#
#nohup ./start.sh >output 2>&1 &

databaseinput='icity_3.8_test'
databaseoutput='ods_icity'
jdbc='jdbc:mysql://172.31.47.227:3306/'${databaseinput}'?serverTimezone=GMT%2B8&useSSL=false&allowPublicKeyRetrieval=true'
password='Ict@123'

tables=(imp_event_info imp_event_handle s_user imp_equipment_warn imp_equipment_monitor t_wechat_config temp_test)

for table in ${tables[@]}
     do
          sqoop import \
          --connect ${jdbc} \
          --username root \
          --password ${password} \
          --table ${table} \
          --hive-import \
          --hive-database ${databaseoutput} \
          --hive-table ${table} \
          --delete-target-dir \
          --fields-terminated-by '\t' \
          --m 1
     done

#
#sqoop import \
#--connect jdbc:mysql://localhost:3306/apr \
#--username root \
#--password root \
#-e "select id, name, age from mon2 where sex='m' and \$CONDITIONS" \
#--target-dir /user/hive/warehouse/hive_part \
#--split-by id \
#--hive-overwrite \
#--hive-import \
#--create-hive-table \
#--hive-partition-key sex \
#--hive-partition-value 'm' \
#--fields-terminated-by ',' \
#--hive-table mar.hive_part \
#--direct

#sqoop import \
#--connect jdbc:mysql://master1.hadoop:3306/test \
#--username root \
#--password 123456 \
#--query 'select * from people_access_log where \$CONDITIONS and url = "https://www.baidu.com"' \
#--target-dir /user/hive/warehouse/web/people_access_log \
#--delete-target-dir \
#--fields-terminated-by '\t' \
#-m 1

#
#增量导入
#sqoop job --create mysql2hive_job -- import \
#--connect jdbc:mysql://master1.hadoop:3306/test \
#--username root \
#--password 123456 \
#--table people_access_log \
#--target-dir /user/hive/warehouse/web.db/people_access_log \
#--check-column id \
#--incremental append \
#--fields-terminated-by '\t' \
#--last-value 6 \
#-m 1