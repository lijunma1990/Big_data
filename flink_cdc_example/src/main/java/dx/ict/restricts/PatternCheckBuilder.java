package dx.ict.restricts;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/6/12 15:51
 * @Version 1.0
 */
public class PatternCheckBuilder {

    private String MSG_RIGHT = "";
    private String MSG_ERROR = "";
    private String Pattern = "";
    private String PatternType = "";

    public PatternCheckBuilder() {
    }

    public void setMSG_RIGHT(String MSG_RIGHT) {
        this.MSG_RIGHT = MSG_RIGHT;
    }

    public void setMSG_ERROR(String MSG_ERROR) {
        this.MSG_ERROR = MSG_ERROR;
    }

    public void setPattern(String pattern) {
        Pattern = pattern;
    }

    public void setPatternType(String patternType) {
        PatternType = patternType;
    }


    public PatternCheck build() {
        return new PatternCheck(this.Pattern, this.MSG_RIGHT, this.MSG_ERROR, this.PatternType);
    }

}
