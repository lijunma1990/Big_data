package dx.ict.restricts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 李二白
 * @date 2023/03/31
 * 数据清洗与数据质量检测配置类
 * 作用：字符串检查是否合规
 */
public class type_check_String implements RestrictRule<String> {

    String Msg;

    public type_check_String() {
        this.Msg = "";
    }

    @Override
    public boolean ifPass(String item) {
        //匹配任意字符
        Pattern pattern = Pattern.compile("^.+$");
        final Matcher matcher = pattern.matcher(item);
        if (matcher.matches()) {
            this.Msg = "String check passed!";
            return true;
        } else {
            this.Msg = "String check not passed!";
            return false;
        }
    }

    @Override
    public String getMsg() {
        return Msg;
    }
}
