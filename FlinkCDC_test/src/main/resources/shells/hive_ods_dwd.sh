#!/bin/bash
db1='dwd_icity'
db2='ods_icity'




hive<<EOF

#create database ${db1};


#create table ${db1}.${table} as select * from ${db2}.${table};

create table ${db1}.imp_event_info as select * from ${db2}.imp_event_info;
create table ${db1}.imp_event_handle as  select * from ${db2}.imp_event_handle;
create table ${db1}.s_user as select * from ${db2}.s_user;
create table ${db1}.imp_equipment_warn as select * from ${db2}.imp_equipment_warn;
create table ${db1}.imp_equipment_monitor as  select * from ${db2}.imp_equipment_monitor;
create table ${db1}.t_wechat_config as select * from ${db2}.t_wechat_config;
create table ${db1}.temp_test as select * from ${db2}.temp_test;

quit;
EOF

#select count(*) from dwd_icity.imp_event_info       ;
#select count(*) from dwd_icity.imp_event_handle     ;
#select count(*) from dwd_icity.s_user               ;
#select count(*) from dwd_icity.imp_equipment_warn   ;
#select count(*) from dwd_icity.imp_equipment_monitor;
#select count(*) from dwd_icity.t_wechat_config      ;
#select count(*) from dwd_icity.temp_test            ;

# create table dwd_icity.imp_event_info as select * from ods_icity.imp_event_info;

# 17852
# select count(*) from ods_icity.imp_event_info;

# drop table dwd_icity.imp_event_info         ;
# drop table dwd_icity.imp_event_handle       ;
# drop table dwd_icity.s_user                 ;
# drop table dwd_icity.imp_equipment_warn     ;
# drop table dwd_icity.imp_equipment_monitor  ;
# drop table dwd_icity.t_wechat_config        ;
# drop table dwd_icity.temp_test              ;

# drop database dwd_icity;   


# drop table imp_event_info         ;
# drop table imp_event_handle       ;
# drop table s_user                 ;
# drop table imp_equipment_warn     ;
# drop table imp_equipment_monitor  ;
# drop table t_wechat_config        ;
# drop table temp_test              ;     

# TRUNCATE TABLE 
# TRUNCATE TABLE imp_event_info       ;
# TRUNCATE TABLE imp_event_handle     ;
# TRUNCATE TABLE s_user               ;
# TRUNCATE TABLE imp_equipment_warn   ;
# TRUNCATE TABLE imp_equipment_monitor;
# TRUNCATE TABLE t_wechat_config      ;
# TRUNCATE TABLE temp_test            ;