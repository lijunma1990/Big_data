package com.keven.mall.realtime.bean;

import lombok.Data;

/**
 * @author KevenHe
 * @create 2022/1/18 16:37
 */
@Data
public class TableProcess {
    public static final String SINK_TYPE_HBASE ="hbase";
    public static final String SINK_TYPE_KAFKA ="kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
