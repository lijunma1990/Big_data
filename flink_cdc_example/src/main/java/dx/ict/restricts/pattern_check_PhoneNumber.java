package dx.ict.restricts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author 李二白
 * @date 2023/03/31
 * 数据清洗与数据质量检测配置类
 * 作用：检查电话号码是否规范
 */

public class pattern_check_PhoneNumber implements RestrictRule<String> {

    String Msg;

    public pattern_check_PhoneNumber() {
        this.Msg = "";
    }

    @Override
    public boolean ifPass(String item) {
        //匹配任意字符
        Pattern pattern = Pattern.compile("^[1]([3-9])[0-9]{9}$");
        final Matcher matcher = pattern.matcher(item);
        if (matcher.matches()) {
            this.Msg = "Phone number check passed!";
            return true;
        } else {
            this.Msg = "Phone number check not passed!";
            return false;
        }
    }

    @Override
    public String getMsg() {
        return Msg;
    }
}
