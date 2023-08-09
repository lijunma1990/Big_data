package dx.ict.restricts;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
/**
 * @author 李二白
 * @date 2023/03/31
 * 数据清洗与数据质量检测配置接口
 * 作用：数据监测实现类模板
 */
@Public
public interface RestrictRule<S> extends Function, Serializable {


    boolean ifPass(S item);

    String getMsg();


}
