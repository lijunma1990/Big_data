package dx.ict.util;

import dx.ict.restricts.*;

/**
 * @author 李二白
 * @date 2023/03/31
 * 数据清洗与数据质量检测器
 * 作用：集中数据监测家口
 */
public class QualityCheck {
    String item;
    RestrictRule<String> rule;

    public QualityCheck(String item, RestrictRule<String> rule) {
        this.item = item;
        this.rule = rule;
    }

    public boolean CheckQuality() {
        return this.rule.ifPass(this.item);
    }

    public String getMsg() {
        return this.rule.getMsg();
    }
}
