package dx.ict.enumeration;

import dx.ict.restricts.RestrictRule;
import dx.ict.restricts.*;


/**
 * @author 李二白
 * @date 2023/04/13
 */
public enum RestrictionEnum {
    type_check_Int("type_check_Int", new type_check_Int()),
    type_check_String("type_check_String", new type_check_String()),
    pattern_check_PhoneNumber("pattern_check_PhoneNumber", new pattern_check_PhoneNumber()),
    pattern_check_lon_and_lat("pattern_check_lon_and_lat",new pattern_check_longitude());

    private final String CheckName;
    private final RestrictRule restrictRule;

    RestrictionEnum(String checkName, RestrictRule restrictRule) {
        CheckName = checkName;
        this.restrictRule = restrictRule;
    }

    public String getCheckName() {
        return CheckName;
    }

    public RestrictRule getRestrictRule() {
        return restrictRule;
    }

    public static String[] getEnumTypes() {
        /*
         * 通过该方法获得枚举的表名称的列表
         * */

        RestrictionEnum[] Enums = RestrictionEnum.values();
        String names = "";
        for (RestrictionEnum item : Enums
        ) {
            names = names.concat(item.getCheckName()).concat(",");
        }
        return names.split(",");
    }

}
