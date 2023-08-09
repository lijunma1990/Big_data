package dx.ict.enumeration;


import org.apache.flink.streaming.api.functions.ProcessFunction;

public enum HiveTableEnum {
    ods_user_info("ods_user_info",
            "CREATE TABLE IF NOT EXISTS `myhive`.`ljm_test`.`ods_user_info` \n" +
                    "(\n" +
                    "`ID` INT, \n" +
                    "`NAME` STRING, \n" +
                    "`PHONE_NUMBER` STRING, \n" +
                    "`pt` string, \n" +
                    "`DELETE_MARK` INT \n" +
                    ")\n" +
                    "PARTITIONED BY (`pt_date` string) \n" +
                    "STORED AS parquet TBLPROPERTIES \n" +
                    "(\n" +
                    //抽取事件时间戳，作为事件时间
                    "'partition.time-extractor.timestamp-pattern'='$pt_date 00:00:00',\n" +
                    //有两种分区提交方式，根据机器处理时间（process-time）和分区时间（partition-time）
                    "'sink.partition-commit.trigger'='partition-time',\n" +
                    //如果每小时一个分区，这个参数可设置为1 h，这样意思就是说数据延后一小时写入hdfs，能够达到数据的真确性，如果分区是天，这个参数也没必要设置了，今天的数据明天才能写入，时效性太差
                    "'sink.partition-commit.delay'='30 s',\n" +
                    //小文件自动合并，1.12版的新特性，解决了实时写hive产生的小文件问题
                    "'auto-compaction'='true',\n" +
                    //合并后的最大文件大小
                    "'compaction.file-size'='128MB',\n" +
                    //压缩方式
                    "'parquet.compression'='GZIP',\n" +
                    //metastore值是专门用于写入hive的，也需要指定success-file
                    //这样检查点触发完数据写入磁盘后会创建_SUCCESS文件以及hive metastore上创建元数据，这样hive才能够对这些写入的数据可查
                    "'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                    ")",
            "CREATE TABLE IF NOT EXISTS `myhive`.`ljm_test`.`ods_user_info_e` \n" +
                    "(\n" +
                    "`ID` INT, \n" +
                    "`NAME` STRING, \n" +
                    "`PHONE_NUMBER` STRING, \n" +
                    "`ERROR_COLUMNS` STRING, \n" +
                    "`ERROR_MESSAGE` STRING, \n" +
                    "`pt` string, \n" +
                    "`DELETE_MARK` INT \n" +
                    ")\n" +
                    "PARTITIONED BY (`pt_date` string) \n" +
                    "STORED AS parquet TBLPROPERTIES \n" +
                    "(\n" +
                    //抽取事件时间戳，作为事件时间
                    "'partition.time-extractor.timestamp-pattern'='$pt_date 00:00:00',\n" +
                    //有两种分区提交方式，根据机器处理时间（process-time）和分区时间（partition-time）
                    "'sink.partition-commit.trigger'='partition-time',\n" +
                    //如果每小时一个分区，这个参数可设置为1 h，这样意思就是说数据延后一小时写入hdfs，能够达到数据的真确性，如果分区是天，这个参数也没必要设置了，今天的数据明天才能写入，时效性太差
                    "'sink.partition-commit.delay'='30 s',\n" +
                    //小文件自动合并，1.12版的新特性，解决了实时写hive产生的小文件问题
                    "'auto-compaction'='true',\n" +
                    //合并后的最大文件大小
                    "'compaction.file-size'='128MB',\n" +
                    //压缩方式
                    "'parquet.compression'='GZIP',\n" +
                    //metastore值是专门用于写入hive的，也需要指定success-file
                    //这样检查点触发完数据写入磁盘后会创建_SUCCESS文件以及hive metastore上创建元数据，这样hive才能够对这些写入的数据可查
                    "'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                    ")"),
    ods_icity_omp_r_equipment_algo_ability("ods_icity_omp_r_equipment_algo_ability",
            "",
            "");

    private final String TableName;
    private final String CreateStatement;
    private final String CreateStatement_E;

    HiveTableEnum(String tableName, String createStatement, String createStatement_E) {
        TableName = tableName;
        CreateStatement = createStatement;
        CreateStatement_E = createStatement_E;
    }

    public String getTableName() {
        return TableName;
    }

    public String getCreateStatement_E() {
        return CreateStatement_E;
    }

    public String getCreateStatement() {
        return CreateStatement;
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举的表名称的列表
         * */

        HiveTableEnum[] Enums = HiveTableEnum.values();
        String names = "";
        for (HiveTableEnum item : Enums
        ) {
            names = names.concat(item.getTableName()).concat(",");
        }
        return names.split(",");
    }
}
