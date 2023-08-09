package dx.ict.deserializationSchema;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/20 10:57
 * @Version 1.0
 */

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

//import org.apache.kafka.connect.data.Field;
//import org.apache.kafka.connect.data.Schema;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;


public class MySqlDebeziumDeserializer_Tuple4 implements DebeziumDeserializationSchema, MySqlDeserialization {

    /**
     * Runtime converter that converts Kafka {@link SourceRecord}s into {@link RowData} consisted of
     * physical column values.
     * example:Tuple2<String,Row>
     */

    private Map<String, RowType> tableRowTypeMap;
    private Map<String, DeserializationRuntimeConverter> physicalConverterMap = Maps.newConcurrentMap();

    public MySqlDebeziumDeserializer_Tuple4(Map tableRowTypeMap) {
        this.tableRowTypeMap = tableRowTypeMap;
        init();
    }

    public MySqlDebeziumDeserializer_Tuple4() {
        this.tableRowTypeMap = Maps.newConcurrentMap();
        init();
    }

    public void setTableRowTypeMap(Map tableRowTypeMap) {
        this.tableRowTypeMap = tableRowTypeMap;
    }

    public void init() {
        for (String tablename : this.tableRowTypeMap.keySet()) {
            RowType rowType = this.tableRowTypeMap.get(tablename);
            //创建反序列化运行转换对象，将rowType转换为相应格式的对象
            DeserializationRuntimeConverter physicalConverter = createNotNullConverter(rowType);
            this.physicalConverterMap.put(tablename, physicalConverter);
        }
    }

    @Override
    public void deserialize(SourceRecord record, Collector out) throws Exception {
        //创建JSON 对象用于封装结果数据
        JSONObject result = new JSONObject();
        JSONObject outJson = new JSONObject();

        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        Struct source = value.getStruct("source");
        String tableName = source.get("table").toString();
        //获取消息时间戳
        String ts_ms = value.get("ts_ms").toString();
        DeserializationRuntimeConverter physicalConverter = physicalConverterMap.get(tableName);
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Row insert = extractAfterRow(value, valueSchema, physicalConverter);
            insert.setKind(RowKind.INSERT);
            String DELETE_MARK = String.valueOf(0);
//            result.put(tableName, insert);
//            out.collect(result.toJSONString());
            final Tuple4<String, Row, String, String> Res = Tuple4.of(tableName, insert, ts_ms, DELETE_MARK);
            out.collect(Res);
        } else if (op == Envelope.Operation.DELETE) {
            Row delete = extractBeforeRow(value, valueSchema, physicalConverter);
//            delete.setKind(RowKind.INSERT);
            delete.setKind(RowKind.DELETE);
            String DELETE_MARK = String.valueOf(1);
//            result.put(tableName, delete);
//            out.collect(result.toJSONString());
            final Tuple4<String, Row, String, String> Res = Tuple4.of(tableName, delete, ts_ms, DELETE_MARK);
            out.collect(Res);
        } else {
            String DELETE_MARK = String.valueOf(0);
            Row before = extractBeforeRow(value, valueSchema, physicalConverter);
//            before.setKind(RowKind.INSERT);
            before.setKind(RowKind.UPDATE_BEFORE);
//            result.put(tableName, before);
//            out.collect(result.toJSONString());
            final Tuple4<String, Row, String, String> Res_bef = Tuple4.of(tableName, before, ts_ms, DELETE_MARK);
            out.collect(Res_bef);
            Row after = extractAfterRow(value, valueSchema, physicalConverter);
//            after.setKind(RowKind.INSERT);
            after.setKind(RowKind.UPDATE_AFTER);
//            result.put(tableName, after);
//            out.collect(result.toJSONString());
            final Tuple4<String, Row, String, String> Res_aft = Tuple4.of(tableName, after, ts_ms, DELETE_MARK);
            out.collect(Res_aft);
        }
    }

    private Row extractAfterRow(Struct value, Schema valueSchema, DeserializationRuntimeConverter physicalConverter) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        JSONObject afterJson = getJsonObject(after);
        return (Row) physicalConverter.convert(after, afterSchema);
//        return afterJson;
    }

    private Row extractBeforeRow(Struct value, Schema valueSchema, DeserializationRuntimeConverter physicalConverter) throws Exception {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        JSONObject beforeJson = getJsonObject(before);
        return (Row) physicalConverter.convert(before, beforeSchema);
//        return beforeJson;
    }


    @Override
    public TypeInformation<Tuple4<String, Row, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple4<String, Row, String, String>>() {
        });
    }


    private JSONObject getJsonObject(Struct struct_) {
        JSONObject Json_object = new JSONObject();
        if (struct_ != null) {
            Schema schema = struct_.schema();
            System.out.println("Schema >>>>>>>>>>>>> " + schema);
            //获取schema信息
            List<Field> fieldList = schema.fields();

            for (Field field : fieldList) {
                Json_object.put(field.name(), struct_.get(field));
            }
        }
        return Json_object;
    }

    public static DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {


        switch (type.getTypeRoot()) {
            case NULL:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return null;
                    }
                };
            case BOOLEAN:
                return convertToBoolean();
            case TINYINT:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return Byte.parseByte(dbzObj.toString());
                    }
                };
            case SMALLINT:
                return new DeserializationRuntimeConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) {
                        return Short.parseShort(dbzObj.toString());
                    }
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return convertToInt();
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return convertToLong();
            case DATE:
                return convertToDate();
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertToTimestamp(ZoneId.of("UTC"));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp(ZoneId.of("UTC"));
            case FLOAT:
                return convertToFloat();
            case DOUBLE:
                return convertToDouble();
            case CHAR:
            case VARCHAR:
                return convertToString();
            case BINARY:
            case VARBINARY:
                return convertToBinary();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ROW:
                return createRowConverter(
                        (RowType) type);
            case ARRAY:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DeserializationRuntimeConverter convertToBoolean() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Boolean) {
                    return dbzObj;
                } else if (dbzObj instanceof Byte) {
                    return (byte) dbzObj == 1;
                } else if (dbzObj instanceof Short) {
                    return (short) dbzObj == 1;
                } else {
                    return Boolean.parseBoolean(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToInt() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return dbzObj;
                } else if (dbzObj instanceof Long) {
                    return ((Long) dbzObj).intValue();
                } else {
                    return Integer.parseInt(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToLong() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return ((Integer) dbzObj).longValue();
                } else if (dbzObj instanceof Long) {
                    return dbzObj;
                } else {
                    return Long.parseLong(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                BigDecimal bigDecimal;
                if (dbzObj instanceof byte[]) {
                    // decimal.handling.mode=precise
                    bigDecimal = Decimal.toLogical((org.apache.kafka.connect.data.Schema) schema, (byte[]) dbzObj);
                } else if (dbzObj instanceof String) {
                    // decimal.handling.mode=string
                    bigDecimal = new BigDecimal((String) dbzObj);
                } else if (dbzObj instanceof Double) {
                    // decimal.handling.mode=double
                    bigDecimal = BigDecimal.valueOf((Double) dbzObj);
                } else {
                    if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                        SpecialValueDecimal decimal =
                                VariableScaleDecimal.toLogical((Struct) dbzObj);
                        bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
                    } else {
                        // fallback to string
                        bigDecimal = new BigDecimal(dbzObj.toString());
                    }
                }
                return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
            }
        };
    }

    private static DeserializationRuntimeConverter convertToDouble() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return ((Float) dbzObj).doubleValue();
                } else if (dbzObj instanceof Double) {
                    return dbzObj;
                } else {
                    return Double.parseDouble(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToFloat() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return dbzObj;
                } else if (dbzObj instanceof Double) {
                    return ((Double) dbzObj).floatValue();
                } else {
                    return Float.parseFloat(dbzObj.toString());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter convertToDate() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
            }
        };
    }

    private static DeserializationRuntimeConverter convertToTime() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case MicroTime.SCHEMA_NAME:
                            return (int) ((long) dbzObj / 1000);
                        case NanoTime.SCHEMA_NAME:
                            return (int) ((long) dbzObj / 1000_000);
                    }
                } else if (dbzObj instanceof Integer) {
                    return dbzObj;
                }
                // get number of milliseconds of the day
                return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
            }
        };
    }

    private static DeserializationRuntimeConverter convertToTimestamp(ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case Timestamp.SCHEMA_NAME:
                            return TimestampData.fromEpochMillis((Long) dbzObj);
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    micro / 1000, (int) (micro % 1000 * 1000));
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (long) dbzObj;
                            return TimestampData.fromEpochMillis(
                                    nano / 1000_000, (int) (nano % 1000_000));
                    }
                }
                LocalDateTime localDateTime =
                        TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
                return TimestampData.fromLocalDateTime(localDateTime);
            }
        };
    }

    private static DeserializationRuntimeConverter convertToLocalTimeZoneTimestamp(
            ZoneId serverTimeZone) {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    // TIMESTAMP_LTZ type is encoded in string type
                    Instant instant = Instant.parse(str);
                    return TimestampData.fromLocalDateTime(
                            LocalDateTime.ofInstant(instant, serverTimeZone));
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + dbzObj
                                + "' of type "
                                + dbzObj.getClass().getName());
            }
        };
    }

    private static DeserializationRuntimeConverter convertToString() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return StringData.fromString(dbzObj.toString());
            }
        };
    }

    private static DeserializationRuntimeConverter convertToBinary() {
        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof byte[]) {
                    return dbzObj;
                } else if (dbzObj instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
                }
            }
        };
    }

    private static DeserializationRuntimeConverter createRowConverter(RowType rowType) {
        final DeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(
                                logicType ->
                                        createNotNullConverter(logicType))
                        .toArray(DeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return new DeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                Struct struct = (Struct) dbzObj;
                int arity = fieldNames.length;
                Row row = new Row(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    Field field = schema.field(fieldName);
                    if (field == null) {
                        row.setField(i, null);
                    } else {
                        Object fieldValue = struct.getWithoutDefault(fieldName);
                        Schema fieldSchema = schema.field(fieldName).schema();
                        Object convertedField =
                                convertField(fieldConverters[i], fieldValue, fieldSchema);
                        row.setField(i, convertedField);
                    }
                }
                return row;
            }
        };
    }

    private static Object convertField(
            DeserializationRuntimeConverter fieldConverter, Object fieldValue, Schema fieldSchema)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, fieldSchema);
        }
    }

}