package dx.ict.restricts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 李二白
 * @date 2023/03/31
 * 数据清洗与数据质量检测配置类
 * 作用：整数检查是否合规
 */
public class type_check_Int implements RestrictRule<String> {

    String Msg;

    public type_check_Int() {
        this.Msg = "";
    }

    @Override
    public boolean ifPass(String item) {
        //匹配整数
        Pattern pattern = Pattern.compile("^[0-9]*$");
        final Matcher matcher = pattern.matcher(item);
        if (matcher.matches()) {
            this.Msg = "Int check passed!";
            return true;
        } else {
            this.Msg = "Int check not passed!";
            return false;
        }
    }

    @Override
    public String getMsg() {
        return Msg;
    }
}
