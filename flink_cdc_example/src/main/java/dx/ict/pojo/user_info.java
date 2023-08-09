package dx.ict.pojo;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 实体类 用于存储数据库中的数据 javabean
 *
 * @author 李二白
 * @date 2023/03/31
 */
public class user_info extends HiveTableClass {
    private int id;
    private String name;
    private String phone_number;

    public user_info() {
        super("0", 0);
        this.id = 0;
        this.name = "";
        this.phone_number = "";
    }


    public user_info(int id,
                     String name,
                     String phone_number,
                     String pt,
                     int DELETE_MARK) {
        super(pt, DELETE_MARK);
        this.id = id;
        this.name = name;
        this.phone_number = phone_number;
    }

    @Override
    public Schema.Builder getLocalSchemaBuilder() {
        return super.getLocalSchemaBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("phone_number", DataTypes.STRING());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        user_info user_info = (user_info) o;
        return id == user_info.id &&
                Objects.equals(name, user_info.name) &&
                Objects.equals(phone_number, user_info.phone_number);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, name, phone_number);
    }

    @Override
    public String toString() {
        return "user_info{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", phone_number='" + phone_number + '\'' +
                '}';
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhone_number() {
        return phone_number;
    }

    public void setPhone_number(String phone_number) {
        this.phone_number = phone_number;
    }
}

