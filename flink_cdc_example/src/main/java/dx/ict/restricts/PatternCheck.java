package dx.ict.restricts;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/6/12 16:39
 * @Version 1.0
 */
public class PatternCheck implements RestrictRule {

    final String MSG_RIGHT;
    final String MSG_ERROR;
    String Pattern;
    String PatternType;
    String Msg;

    public PatternCheck(String pattern, String MSG_RIGHT, String MSG_ERROR, String patternType) {
        this.MSG_RIGHT = MSG_RIGHT;
        this.MSG_ERROR = MSG_ERROR;
        this.Pattern = pattern;
        this.PatternType = patternType;
        this.Msg = "";
    }

    @Override
    public boolean ifPass(Object item) {
        //匹配任意字符
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(this.Pattern);
        final Matcher matcher = pattern.matcher(item.toString());
        if (matcher.matches()) {
            this.Msg = this.MSG_RIGHT;
            return true;
        } else {
            this.Msg = this.MSG_ERROR;
            return false;
        }
    }

    @Override
    public String getMsg() {
        return this.Msg;
    }

    public String getPatternType() {
        return PatternType;
    }
}
