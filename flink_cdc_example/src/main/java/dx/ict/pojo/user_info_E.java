package dx.ict.pojo;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

/**
 * @author 李二白
 * @date 2023/04/12
 * 表user_info对应表的数据质量检测错误类别收集表：user_info_e
 */
public class user_info_E extends user_info {
    String ERROR_COLUMNS;
    String ERROR_MESSAGE;


    public user_info_E() {
        super(0, "", "", "0", 0);
        this.ERROR_COLUMNS = "";
        this.ERROR_MESSAGE = "";
    }

    public user_info_E(String ERROR_COLUMNS, String ERROR_MESSAGE) {
        super(0, "", "", "0", 0);
        this.ERROR_COLUMNS = ERROR_COLUMNS;
        this.ERROR_MESSAGE = ERROR_MESSAGE;
    }

    public user_info_E(int id, String name, String phone_number, String pt, String ERROR_COLUMNS, String ERROR_MESSAGE, int DELETE_MARK) {
        super(id, name, phone_number, pt, DELETE_MARK);
        this.ERROR_COLUMNS = ERROR_COLUMNS;
        this.ERROR_MESSAGE = ERROR_MESSAGE;
    }

    public String getERROR_COLUMNS() {
        return ERROR_COLUMNS;
    }

    public void setERROR_COLUMNS(String ERROR_COLUMNS) {
        this.ERROR_COLUMNS = ERROR_COLUMNS;
    }

    public String getERROR_MESSAGE() {
        return ERROR_MESSAGE;
    }

    public void setERROR_MESSAGE(String ERROR_MESSAGE) {
        this.ERROR_MESSAGE = ERROR_MESSAGE;
    }

    @Override
    public String toString() {
        return "user_info_E{" +
                "ERROR_COLUMNS='" + ERROR_COLUMNS + '\'' +
                ", ERROR_MESSAGE='" + ERROR_MESSAGE + '\'' +
                '}';
    }

    @Override
    public Schema.Builder getLocalSchemaBuilder() {
        return super.getLocalSchemaBuilder()
                .column("ERROR_COLUMNS", DataTypes.STRING())
                .column("ERROR_MESSAGE", DataTypes.STRING());
    }

}
