package dx.ict.restricts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class pattern_check_longitude implements RestrictRule<String> {
    String Msg;

    public pattern_check_longitude() {
        Msg = "";
    }

    @Override
    public boolean ifPass(String item) {
        //匹配任意字符
        Pattern pattern = Pattern.compile("^(\\-|\\+)?(((\\d|[1-9]\\d|1[0-7]\\d|0{1,3})\\.\\d{0,6})|(\\d|[1-9]\\d|1[0-7]\\d|0{1,3})|180\\.0{0,6}|180)$");
        final Matcher matcher = pattern.matcher(item);
        if (matcher.matches()) {
            this.Msg = "longitude check passed!";
            return true;
        } else {
            this.Msg = "longitude check not passed!";
            return false;
        }
    }

    @Override
    public String getMsg() {
        return Msg;
    }
}
