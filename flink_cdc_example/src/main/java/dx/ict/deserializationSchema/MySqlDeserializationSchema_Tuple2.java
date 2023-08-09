package dx.ict.deserializationSchema;


import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


/**
 * @author 李二白
 * @date 2023/03/24
 */

/**
 * 官方默认String类型的数据样式
 * SourceRecord{sourcePartition={server=mysql_binlog_source},
 * sourceOffset={transaction_id=null, ts_sec=1673531342,
 * file=mysql-bin.000001, pos=803, row=1, server_id=1, event=2}}
 * ConnectRecord{topic='mysql_binlog_source.cdc_test.user_info',
 * kafkaPartition=null, key=Struct{id=1003},
 * keySchema=Schema{mysql_binlog_source.cdc_test.user_info.Key:STRUCT},
 * value=Struct{before=Struct{id=1003,name=wangwu,sex=famale},after=Struct{id=1003,name=wangwu,sex=male},
 * source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1673531342000,db=cdc_test,table=user_info,server_id=1,file=mysql-bin.000001,pos=943,row=0},op=u,ts_ms=1673531338605},
 * valueSchema=Schema{mysql_binlog_source.cdc_test.user_info.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * <p>
 * JsonDebeziumDeserializationSchema默认的String类型的数据样式
 * {
 * "before":null,
 * "after":{"id":0,"name":"daima0201","phone_number":"15312626883"},
 * "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"ljm_test","sequence":null,"table":"user_info","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
 * "op":"r",
 * "ts_ms":1679819427913,
 * "transaction":null}
 */
public class MySqlDeserializationSchema_Tuple2 implements DebeziumDeserializationSchema<Tuple2<String, String>>, MySqlDeserialization {


    /**
     * TODO 明确自己想要的数据格式
     * (
     * "db":"",
     * "tableName":"",
     * "before":{"id":"1001","name":""...},
     * "after":{"id":"1001","name":""...},
     * "op":""
     * )
     * SourceRecord{
     * sourcePartition={server=mysql_binlog_source},
     * sourceOffset={file=mysql-bin.000063, pos=154}
     * }
     * ConnectRecord{
     * topic='mysql_binlog_source.wudl-gmall.user_info',
     * kafkaPartition=null,
     * key=Struct{id=4000},
     * keySchema=Schema{mysql_binlog_source.wudl_gmall.user_info.Key:STRUCT},
     * value=Struct{
     * after=Struct{id=4000,login_name=i0v0k9,nick_name=xxx,name=xxx,phone_num=137xxxxx,email=xxxx@qq.com,user_level=1,birthday=1969-12-04,gender=F,create_time=2020-12-04 23:28:45},
     * source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=last,db=wudl-gmall,table=user_info,server_id=0,file=mysql-bin.000063,pos=154,row=0},
     * op=c,ts_ms=1636255826014
     * },
     * valueSchema=Schema{mysql_binlog_source.wudl_gmall.user_info.Envelope:STRUCT},
     * timestamp=null,
     * headers=ConnectHeaders(headers=)
     * }
     */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple2<String, String>> collector) throws Exception {
        //创建JSON 对象用于封装结果数据
        JSONObject result = new JSONObject();

        //获取库名&表名
        String topic = sourceRecord.topic();//根据上面样式通过sourceRecord.键值，获取对应值
        //获取结果：topic='mysql_binlog_source.cdc_test.user_info'
        String[] fields = topic.split("\\.");//安装`.`进行分割（.需要转义）

        //添加对应的库名和表名键值对
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);


        //获取before数据
        Struct value = (Struct) sourceRecord.value();//需要进行强转下,注意导的是：org.apache.kafka.connect.data.Struct这个包
        Struct before = value.getStruct("before");//通过指定before，获取before字段的数据
//        System.out.println("Before >>>>>>>>>>>>> " + before);
        JSONObject beforeJson = getJsonObject(before);
        result.put("before", beforeJson);//把before信息添加进去

        //同理获取after
        Struct after = value.getStruct("after");
        JSONObject afterJson = getJsonObject(after);
//        System.out.println("After >>>>>>>>>>>>> " + after);
        result.put("after", afterJson);

        //获取操作类型(operation不能直接通过sourceRecord获取)
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);//注意导包：io.debezium.data.Envelope
        //将operation信息添加进去
        result.put("op", operation);

        //获取消息时间戳
        String ts_ms = value.get("ts_ms").toString();
//        System.out.println("ts_ms >>>>>>>>>>>>> " + ts_ms);
        result.put("ts_ms", ts_ms);

        //输出数据
        collector.collect(Tuple2.of(fields[2], result.toJSONString()));
    }

    private JSONObject getJsonObject(Struct struct_) {
        JSONObject Json_object = new JSONObject();
        if (struct_ != null) {
            Schema schema = struct_.schema();
//            System.out.println("Schema >>>>>>>>>>>>> " + schema);
            //获取schema信息
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                Json_object.put(field.name(), struct_.get(field));
            }
        }
        return Json_object;
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }

    @Override
    public void init() {

    }

    @Override
    public void setTableRowTypeMap(Map tableRowTypeMap) {

    }
}
