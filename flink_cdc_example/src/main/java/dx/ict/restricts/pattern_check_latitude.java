package dx.ict.restricts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class pattern_check_latitude implements RestrictRule<String> {
    String Msg;

    public pattern_check_latitude() {
        Msg = "";
    }

    @Override
    public boolean ifPass(String item) {
        //匹配任意字符
        Pattern pattern = Pattern.compile("^(\\-|\\+)?([0-8]?\\d{1}\\.\\d{0,6}|90\\.0{0,6}|[0-8]?\\d{1}|90)$");
        final Matcher matcher = pattern.matcher(item);
        if (matcher.matches()) {
            this.Msg = "latitude check passed!";
            return true;
        } else {
            this.Msg = "latitude check not passed!";
            return false;
        }
    }

    @Override
    public String getMsg() {
        return Msg;
    }
}
