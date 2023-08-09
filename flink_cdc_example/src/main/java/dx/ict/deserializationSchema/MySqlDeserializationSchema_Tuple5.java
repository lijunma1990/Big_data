package dx.ict.deserializationSchema;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/5 14:41
 * @Version 1.0
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
public class MySqlDeserializationSchema_Tuple5 implements DebeziumDeserializationSchema, MySqlDeserialization {


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
    public void deserialize(SourceRecord record, Collector out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        Struct source = value.getStruct("source");
        String tableName = source.get("table").toString();
        //获取消息时间戳
        String ts_ms = value.get("ts_ms").toString();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Tuple3<JSONObject, JSONObject, JSONObject> insert = extractAfterRow(value, valueSchema);
            String DELETE_MARK = String.valueOf(0);
            final JSONObject out_json = new JSONObject();
            out_json.put("value", insert.f0);
            out_json.put("index", insert.f1);
            out_json.put("field", insert.f2);
            final String insert_String = JSONObject.toJSONString(out_json, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullListAsEmpty);
            final Tuple5<String, String, String, String, String> Res = Tuple5.of(tableName, "+I", insert_String, ts_ms, DELETE_MARK);
            out.collect(Res);
        } else if (op == Envelope.Operation.DELETE) {
            Tuple3<JSONObject, JSONObject, JSONObject> delete = extractBeforeRow(value, valueSchema);
            String DELETE_MARK = String.valueOf(1);
            final JSONObject out_json = new JSONObject();
            out_json.put("value", delete.f0);
            out_json.put("index", delete.f1);
            out_json.put("field", delete.f2);
            final String delete_String = JSONObject.toJSONString(out_json, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullListAsEmpty);
            final Tuple5<String, String, String, String, String> Res = Tuple5.of(tableName, "-D", delete_String, ts_ms, DELETE_MARK);
            out.collect(Res);
        } else {
            String DELETE_MARK = String.valueOf(0);
            Tuple3<JSONObject, JSONObject, JSONObject> before = extractBeforeRow(value, valueSchema);
            final JSONObject out_json_bef = new JSONObject();
            out_json_bef.put("value", before.f0);
            out_json_bef.put("index", before.f1);
            out_json_bef.put("field", before.f2);
            final String before_String = JSONObject.toJSONString(out_json_bef, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullListAsEmpty);
            final Tuple5<String, String, String, String, String> Res_bef = Tuple5.of(tableName, "-U", before_String, ts_ms, DELETE_MARK);
//            out.collect(Res_bef);
            Tuple3<JSONObject, JSONObject, JSONObject> after = extractAfterRow(value, valueSchema);
            final JSONObject out_json_aft = new JSONObject();
            out_json_aft.put("value", after.f0);
            out_json_aft.put("index", after.f1);
            out_json_aft.put("field", after.f2);
            final String after_String = JSONObject.toJSONString(out_json_aft, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteNullListAsEmpty);
            final Tuple5<String, String, String, String, String> Res_aft = Tuple5.of(tableName, "+U", after_String, ts_ms, DELETE_MARK);
            out.collect(Res_aft);
        }
    }

    private Tuple3<JSONObject, JSONObject, JSONObject> extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        final Tuple3<JSONObject, JSONObject, JSONObject> after_Tuple = getJsonObject(after);
        JSONObject afterJson = after_Tuple.f0;
        JSONObject Json_index = after_Tuple.f1;
        JSONObject Json_field = after_Tuple.f2;
        return new Tuple3(afterJson, Json_index, Json_field);
    }

    private Tuple3<JSONObject, JSONObject, JSONObject> extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        final Tuple3<JSONObject, JSONObject, JSONObject> before_Tuple = getJsonObject(before);
        JSONObject beforeJson = before_Tuple.f0;
        JSONObject Json_index = before_Tuple.f1;
        JSONObject Json_field = before_Tuple.f2;
        return new Tuple3(beforeJson, Json_index, Json_field);
    }

    private Tuple3<JSONObject, JSONObject, JSONObject> getJsonObject(Struct struct_) {
        JSONObject Json_object = new JSONObject();
        JSONObject Json_index = new JSONObject();
        JSONObject Json_field = new JSONObject();
        if (struct_ != null) {
            Schema schema = struct_.schema();
//            System.out.println("Schema >>>>>>>>>>>>> " + schema);
            //获取schema信息
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                Json_object.put(field.name(), struct_.get(field));
                Json_index.put(String.valueOf(field.index()), field.name());
                Json_field.put(field.name(), field.schema().toString());
            }
        }
        return new Tuple3(Json_object, Json_index, Json_field);
    }

    @Override
    public void init() {

    }

    @Override
    public void setTableRowTypeMap(Map tableRowTypeMap) {

    }

    @Override
    public TypeInformation<Tuple5<String, String, String, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple5<String, String, String, String, String>>() {
        });
    }
}
