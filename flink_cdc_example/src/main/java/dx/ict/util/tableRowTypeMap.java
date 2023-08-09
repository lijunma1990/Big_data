package dx.ict.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.DefaultCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/4/28 15:37
 * @Version 1.0
 */
public class tableRowTypeMap {

    List<String> tables = new ArrayList<>();
    MySqlCatalog mysqlCatalog;
    String tableList;
    String database;
    List<String> primaryKeys;
    // 创建表名和对应RowTypeInfo映射的Map
    Map<String, RowTypeInfo> tableTypeInformationMap = Maps.newConcurrentMap();
    Map<String, DataType[]> tableDataTypesMap = Maps.newConcurrentMap();
    Map<String,String[]> fieldNamesMap = Maps.newConcurrentMap();
    /**
     * 关于fields的逻辑序列，包括了field的名字、类型、描述（可选），对于一张表而言，其row type是对该表行类型的最具体的
     * 特定描述，因此该行的每一列都和row type的fields中对应顺序列相对应，
     */
    Map<String, RowType> tableRowTypeMap = Maps.newConcurrentMap();

    public tableRowTypeMap(
            MySqlCatalog mysqlCatalog,
            String tableList,
            String databaseList
    ) {
        this.mysqlCatalog = mysqlCatalog;
        this.tableList = tableList;
        this.database = databaseList;
        init();
    }

    private void init() {
        //初始化
        // 如果整库同步，则从catalog里取所有表，否则从指定表中取表名
        if (".*".equals(tableList)) {
            try {
                tables = mysqlCatalog.listTables(database);
            } catch (DatabaseNotExistException e) {
                e.printStackTrace();
            }
        } else {
            String[] tableArray = tableList.split(",");
            for (String table : tableArray) {
                tables.add(table.split("\\.")[1]);
            }
        }
        //打印源库中的表
        System.out.println("Source database tables : >>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println(tables);
        for (String table : tables) {
            // 获取mysql catalog中注册的表
            ObjectPath objectPath = new ObjectPath(database, table);
            DefaultCatalogTable catalogBaseTable = null;
            try {
                catalogBaseTable = (DefaultCatalogTable) mysqlCatalog.getTable(objectPath);
            } catch (TableNotExistException e) {
                e.printStackTrace();
            }
            // 获取表的Schema
            Schema schema = catalogBaseTable.getUnresolvedSchema();
            // 获取表中字段名列表
            String[] fieldNames = new String[schema.getColumns().size()];
            // 获取DataType
            DataType[] fieldDataTypes = new DataType[schema.getColumns().size()];
            LogicalType[] logicalTypes = new LogicalType[schema.getColumns().size()];
            // 获取表字段类型
            TypeInformation<?>[] fieldTypes = new TypeInformation[schema.getColumns().size()];
            // 获取表的主键
            primaryKeys = schema.getPrimaryKey().get().getColumnNames();

            for (int i = 0; i < schema.getColumns().size(); i++) {
                Schema.UnresolvedPhysicalColumn column = (Schema.UnresolvedPhysicalColumn) schema.getColumns().get(i);
                fieldNames[i] = column.getName();
                fieldDataTypes[i] = (DataType) column.getDataType();
                fieldTypes[i] = InternalTypeInfo.of(((DataType) column.getDataType()).getLogicalType());
                logicalTypes[i] = ((DataType) column.getDataType()).getLogicalType();
            }
            RowType rowType = RowType.of(logicalTypes, fieldNames);
            tableDataTypesMap.put(table,fieldDataTypes);
            tableRowTypeMap.put(table, rowType);
            tableTypeInformationMap.put(table, new RowTypeInfo(fieldTypes, fieldNames));
            fieldNamesMap.put(table,fieldNames);
        }
    }

    public List<String> getTables() {
        return tables;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, RowTypeInfo> getTableTypeInformationMap() {
        return tableTypeInformationMap;
    }

    public Map<String, DataType[]> getTableDataTypesMap() {
        return tableDataTypesMap;
    }

    public Map<String, RowType> getTableRowTypeMap() {
        return tableRowTypeMap;
    }

    public Map<String, String[]> getFieldNamesMap() {
        return fieldNamesMap;
    }
}
