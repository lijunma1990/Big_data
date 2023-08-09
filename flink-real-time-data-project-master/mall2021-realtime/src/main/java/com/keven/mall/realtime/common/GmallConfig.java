package com.keven.mall.realtime.common;

/**
 * @author KevenHe
 * @create 2022/1/19 21:06
 */
public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:192.168.10.102:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
