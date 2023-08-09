package dx.ict.pojo;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.util.Objects;

/**
 * 实体类 用于存储数据库中的数据 javabean
 *
 * @author 李二白
 * @date 2023/04/06
 */
public class omp_r_equipment_algo_ability extends HiveTableClass {

    private String id;//ID
    private String equipment_id;//设备id
    private String ability_code;//能力编码
    private String belong_dept_id;//组织id
    private String belong_lessee_id;//租户id

    public omp_r_equipment_algo_ability(
    ) {
        /*
         * pojo类，构造函数
         * 映射表：->
         * omp_r_equipment_algo_ability
         * */
        super(TimeStamp.getCurrentTime().toString(),0);
        this.id = "";
        this.equipment_id = "";
        this.ability_code = "";
        this.belong_dept_id = "";
        this.belong_lessee_id = "";
    }


    public omp_r_equipment_algo_ability(
            String id,
            String equipment_id,
            String ability_code,
            String belong_dept_id,
            String belong_lessee_id,
            String pt,
            int DELETE_MARK
    ) {
        /*
         * pojo类，构造函数
         * 映射表：->
         * omp_r_equipment_algo_ability
         * */
        super(pt,DELETE_MARK);
        this.id = id;
        this.equipment_id = equipment_id;
        this.ability_code = ability_code;
        this.belong_dept_id = belong_dept_id;
        this.belong_lessee_id = belong_lessee_id;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        omp_r_equipment_algo_ability that = (omp_r_equipment_algo_ability) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(equipment_id, that.equipment_id) &&
                Objects.equals(ability_code, that.ability_code) &&
                Objects.equals(belong_dept_id, that.belong_dept_id) &&
                Objects.equals(belong_lessee_id, that.belong_lessee_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, equipment_id, ability_code, belong_dept_id, belong_lessee_id);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEquipment_id() {
        return equipment_id;
    }

    public void setEquipment_id(String equipment_id) {
        this.equipment_id = equipment_id;
    }

    public String getAbility_code() {
        return ability_code;
    }

    public void setAbility_code(String ability_code) {
        this.ability_code = ability_code;
    }

    public String getBelong_dept_id() {
        return belong_dept_id;
    }

    public void setBelong_dept_id(String belong_dept_id) {
        this.belong_dept_id = belong_dept_id;
    }

    public String getBelong_lessee_id() {
        return belong_lessee_id;
    }

    public void setBelong_lessee_id(String belong_lessee_id) {
        this.belong_lessee_id = belong_lessee_id;
    }

    @Override
    public Schema.Builder getLocalSchemaBuilder() {
        return super.getLocalSchemaBuilder()
                .column("id", DataTypes.STRING())
                .column("equipment_id", DataTypes.STRING())
                .column("ability_code", DataTypes.STRING())
                .column("belong_dept_id", DataTypes.STRING())
                .column("belong_lessee_id", DataTypes.STRING());
    }

    @Override
    public String toString() {
        return "omp_r_equipment_algo_ability{" +
                "id='" + id + '\'' +
                ", equipment_id='" + equipment_id + '\'' +
                ", ability_code='" + ability_code + '\'' +
                ", belong_dept_id='" + belong_dept_id + '\'' +
                ", belong_lessee_id='" + belong_lessee_id + '\'' +
                '}';
    }
}
