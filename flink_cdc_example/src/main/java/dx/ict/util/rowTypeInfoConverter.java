package dx.ict.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.util.Arrays;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/5/9 9:21
 * @Version 1.0
 */

public class rowTypeInfoConverter {
    RowTypeInfo rowTypeInfo = null;

    public rowTypeInfoConverter(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    public rowTypeInfoConverter() {
    }

    public RowTypeInfo hiveTypeInfoGen(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
        return hiveTypeInfoGen();
    }

    public RowTypeInfo hiveTypeInfoGen() {
        RowTypeInfo rowTypeInfo = TypeInfoGen();
        TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        String[] fieldNames = rowTypeInfo.getFieldNames();
        // 创建一个新的字段类型数组，包括旧的字段类型和新的字段类型
        TypeInformation<?>[] newFieldTypes = Arrays.copyOf(fieldTypes, fieldTypes.length + 3);
        newFieldTypes[fieldTypes.length] = Types.STRING;
//        newFieldTypes[fieldTypes.length + 1] = Types.STRING;
        newFieldTypes[fieldTypes.length + 1] = Types.STRING;
        newFieldTypes[fieldTypes.length + 2] = Types.STRING;
        // 创建一个新的字段名称数组，包括旧的字段名称和新的字段名称
        String[] newFieldNames = Arrays.copyOf(fieldNames, fieldNames.length + 3);
        newFieldNames[fieldNames.length] = "pt";
        newFieldNames[fieldNames.length + 1] = "delete_mark";
        newFieldNames[fieldNames.length + 2] = "pt_date";
        // 返回一个新的 RowTypeInfo 对象
        return new RowTypeInfo(newFieldTypes, newFieldNames);
    }

    public RowTypeInfo TypeInfoGen() {
        TypeInformation<?>[] fieldTypes = this.rowTypeInfo.getFieldTypes();
        String[] fieldNames = this.rowTypeInfo.getFieldNames();
        // 创建一个新的字段名称数组，包括旧的字段名称和新的字段名称
        String[] newFieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
        // 创建一个新的字段类型数组，包括旧的字段类型和新的字段类型
        //遍历newFieldTypes和newFieldNames，并给其赋予新的值
        TypeInformation<?>[] newFieldTypes = TypeInformationConvert(fieldTypes);

        return new RowTypeInfo(newFieldTypes, newFieldNames);
    }

    public RowTypeInfo TypeInfoGen(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
        return TypeInfoGen();
    }

    public Schema SchemaGen(RowTypeInfo rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
        return SchemaGen();
    }

    private Schema SchemaGen() {
        final RowTypeInfo newRowTypeInfo = TypeInfoGen();
        TypeInformation<?>[] fieldTypes = newRowTypeInfo.getFieldTypes();
        String[] fieldNames = newRowTypeInfo.getFieldNames();
        final int length = fieldNames.length;
        Schema.Builder SchemaBuilder = Schema.newBuilder();
        for (int i = 0; i < length; i++) {
            SchemaAppend(SchemaBuilder, fieldNames[i], fieldTypes[i]);
        }
        return SchemaBuilder.build();
    }

    // TypeInformation为字段对应的数据类型
    public static TypeInformation[] TypeInformationConvert(TypeInformation[] srcFieldTypes) {
//        String[] tokens = srcFieldTypes.split(",");
        TypeInformation[] fieldTypes = new TypeInformation[srcFieldTypes.length];
        for (int i = 0, len = srcFieldTypes.length; i < len; i++) {
            String typeName = srcFieldTypes[i].getTypeClass().getSimpleName().toLowerCase();
            fieldTypes[i] = mysqlToHiveType(typeName);
        }
        return fieldTypes;
    }

    //将mysql中的数据类型转换为hive中的数据类型
    public static TypeInformation mysqlToHiveType(String mysqlType) {
        if (mysqlType.startsWith("varchar") || mysqlType.startsWith("char")) {
            return Types.STRING;
        } else if (mysqlType.startsWith("int") || mysqlType.startsWith("tinyint") ||
                mysqlType.startsWith("smallint") || mysqlType.startsWith("mediumint") ||
                mysqlType.startsWith("bigint")) {
            return Types.STRING;
        } else if (mysqlType.startsWith("float") || mysqlType.startsWith("double") ||
                mysqlType.startsWith("decimal")) {
            return Types.STRING;
        } else if (mysqlType.startsWith("datetime") || mysqlType.startsWith("timestamp") ||
                mysqlType.startsWith("date") || mysqlType.startsWith("time")) {
            return Types.STRING;
        } else if (mysqlType.startsWith("binary") || mysqlType.startsWith("varbinary") ||
                mysqlType.startsWith("blob") || mysqlType.startsWith("tinyblob") ||
                mysqlType.startsWith("mediumblob") || mysqlType.startsWith("longblob")) {
            return Types.STRING;
        } else {
            return Types.STRING;
        }
    }
//    public static TypeInformation mysqlToHiveType(String mysqlType) {
//        if (mysqlType.startsWith("varchar") || mysqlType.startsWith("char")) {
//            return Types.STRING;
//        } else if (mysqlType.startsWith("int") || mysqlType.startsWith("tinyint") ||
//                mysqlType.startsWith("smallint") || mysqlType.startsWith("mediumint") ||
//                mysqlType.startsWith("bigint")) {
//            return Types.INT;
//        } else if (mysqlType.startsWith("float") || mysqlType.startsWith("double") ||
//                mysqlType.startsWith("decimal")) {
//            return Types.DOUBLE;
//        } else if (mysqlType.startsWith("datetime") || mysqlType.startsWith("timestamp") ||
//                mysqlType.startsWith("date") || mysqlType.startsWith("time")) {
//            return Types.STRING;
//        } else if (mysqlType.startsWith("binary") || mysqlType.startsWith("varbinary") ||
//                mysqlType.startsWith("blob") || mysqlType.startsWith("tinyblob") ||
//                mysqlType.startsWith("mediumblob") || mysqlType.startsWith("longblob")) {
//            return Types.STRING;
//        } else {
//            return Types.STRING;
//        }

//    }


    // TypeInformation为字段对应的数据类型
    public static Schema.Builder SchemaAppend(Schema.Builder SchemaBuilder, String fieldName, TypeInformation FieldType) {
        String typeName = FieldType.getTypeClass().getSimpleName().toLowerCase();

        if (typeName.startsWith("varchar") || typeName.startsWith("char")) {
            SchemaBuilder.column(fieldName, DataTypes.STRING());
        } else if (typeName.startsWith("int") || typeName.startsWith("tinyint") ||
                typeName.startsWith("smallint") || typeName.startsWith("mediumint") ||
                typeName.startsWith("bigint")) {
            SchemaBuilder.column(fieldName, DataTypes.STRING());
        } else if (typeName.startsWith("float") || typeName.startsWith("double") ||
                typeName.startsWith("decimal")) {
            SchemaBuilder.column(fieldName, DataTypes.STRING());
        } else if (typeName.startsWith("datetime") || typeName.startsWith("timestamp") ||
                typeName.startsWith("date") || typeName.startsWith("time")) {
            SchemaBuilder.column(fieldName, DataTypes.STRING());
        } else if (typeName.startsWith("binary") || typeName.startsWith("varbinary") ||
                typeName.startsWith("blob") || typeName.startsWith("tinyblob") ||
                typeName.startsWith("mediumblob") || typeName.startsWith("longblob")) {
            SchemaBuilder.column(fieldName, DataTypes.STRING());
        } else {
            SchemaBuilder.column(fieldName, DataTypes.STRING());
        }
        return SchemaBuilder;
    }
//    public static Schema.Builder SchemaAppend(Schema.Builder SchemaBuilder, String fieldName, TypeInformation FieldType) {
//        String typeName = FieldType.getTypeClass().getSimpleName().toLowerCase();
//
//        if (typeName.startsWith("varchar") || typeName.startsWith("char")) {
//            SchemaBuilder.column(fieldName, DataTypes.STRING());
//        } else if (typeName.startsWith("int") || typeName.startsWith("tinyint") ||
//                typeName.startsWith("smallint") || typeName.startsWith("mediumint") ||
//                typeName.startsWith("bigint")) {
//            SchemaBuilder.column(fieldName, DataTypes.INT());
//        } else if (typeName.startsWith("float") || typeName.startsWith("double") ||
//                typeName.startsWith("decimal")) {
//            SchemaBuilder.column(fieldName, DataTypes.DOUBLE());
//        } else if (typeName.startsWith("datetime") || typeName.startsWith("timestamp") ||
//                typeName.startsWith("date") || typeName.startsWith("time")) {
//            SchemaBuilder.column(fieldName, DataTypes.STRING());
//        } else if (typeName.startsWith("binary") || typeName.startsWith("varbinary") ||
//                typeName.startsWith("blob") || typeName.startsWith("tinyblob") ||
//                typeName.startsWith("mediumblob") || typeName.startsWith("longblob")) {
//            SchemaBuilder.column(fieldName, DataTypes.STRING());
//        } else {
//            SchemaBuilder.column(fieldName, DataTypes.STRING());
//        }
//        return SchemaBuilder;
//    }

}
