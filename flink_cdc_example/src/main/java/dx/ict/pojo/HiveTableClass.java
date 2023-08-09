package dx.ict.pojo;


import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

/**
 * @author 李二白
 * @date 2023/04/06
 * hive表对应pojo的父类，相应pojo类创建标准，以此为范例
 */
public class HiveTableClass {
    private int DELETE_MARK;
    private String pt;
    private String pt_date;
    private String pt_hour;
    private String pt_min;

    public HiveTableClass(String pt, int DELETE_MARK) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter hourFormat = DateTimeFormatter.ofPattern("HH");
        DateTimeFormatter minuteFormat = DateTimeFormatter.ofPattern("mm");
        this.pt = pt;
        this.DELETE_MARK = DELETE_MARK;
        final LocalDateTime date = new Timestamp(Long.parseLong(this.pt)).toLocalDateTime();
//        System.out.println(date);
        this.pt_date = dateFormat.format(date);
        this.pt_hour = hourFormat.format(date);
        this.pt_min = minuteFormat.format(date);
//        this.pt_date = convertTimestampToDateFormat(pt,"");

    }

    public Schema.Builder getLocalSchemaBuilder() {
        return Schema.newBuilder()
                .column("pt", DataTypes.STRING())
                .column("pt_date", DataTypes.STRING())
                .column("pt_hour", DataTypes.STRING())
                .column("pt_min", DataTypes.STRING())
                .column("DELETE_MARK", DataTypes.INT());
    }

    public int getDELETE_MARK() {
        return DELETE_MARK;
    }

    public void setDELETE_MARK(int DELETE_MARK) {
        this.DELETE_MARK = DELETE_MARK;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }

    public String getPt() {
        return pt;
    }

    public String getPt_date() {
        return pt_date;
    }

    public void setPt_date(String pt_date) {
        this.pt_date = pt_date;
    }

    public String getPt_hour() {
        return pt_hour;
    }

    public void setPt_hour(String pt_hour) {
        this.pt_hour = pt_hour;
    }

    public String getPt_min() {
        return pt_min;
    }

    public void setPt_min(String pt_min) {
        this.pt_min = pt_min;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HiveTableClass that = (HiveTableClass) o;
        return DELETE_MARK == that.DELETE_MARK &&
                Objects.equals(pt, that.pt) &&
                Objects.equals(pt_date, that.pt_date) &&
                Objects.equals(pt_hour, that.pt_hour) &&
                Objects.equals(pt_min, that.pt_min);
    }

    @Override
    public int hashCode() {
        return Objects.hash(DELETE_MARK, pt, pt_date, pt_hour, pt_min);
    }

    @Override
    public String toString() {
        return "HiveTableClass{" +
                "DELETE_MARK=" + DELETE_MARK +
                ", pt='" + pt + '\'' +
                ", pt_date='" + pt_date + '\'' +
                ", pt_hour='" + pt_hour + '\'' +
                ", pt_min='" + pt_min + '\'' +
                '}';
    }
}
