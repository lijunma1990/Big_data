package dx.ict.test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternTest {
    public static void main(String[] args) {
        final String tst_String = "timestamp(1592323200L)";
        final String[] tst_array = "1488149715.0,0416453755,N868744036505448J,47180911_0A00E6FA,869768046634277,47180921_0A010419,wylflKdtA1CBLVOBQAQ06K,3769777382,47180821_0A00CCA4,47180921_0A00FF81,1490157723".split(",");
        String msg = "(imp_equipment_warn,+I[0160FACB1B0A48C8BAEBDD901E6076DD, 7684368443f1400c8365cd1f6de1ca74, S45, E7F0CB8EFAB8453F9FE6770B4CC0EE93, null, null, T37W36, 1, 发现垃圾滞留，请及时处理！, 2023-02-20T03:37:33, 2023-02-20T03:37:33, {\n" +
                "  \"warn_description\" : \"发现垃圾滞留，请及时处理！\",\n" +
                "  \"firm_code\" : \"IVC_Supplier\",\n" +
                "  \"next_report_time\" : \"2023-02-20 11:37:33\",\n" +
                "  \"belong_dept_id\" : \"5E5980A1DF3E4A86B01CDEDB5E4E8445\",\n" +
                "  \"device_report_time\" : \"2023-02-20 03:37:33\",\n" +
                "  \"create_time\" : \"2023-02-20 03:37:33\",\n" +
                "  \"apikey\" : \"D5A10A459451B572BC95D741AFC573B0C6F5D3E5\",\n" +
                "  \"data_id\" : \"8DB952FE1DC949FB891C244DE967EFE3\",\n" +
                "  \"device_code\" : \"3YSCA5542300W6S\",\n" +
                "  \"platform_report_time\" : \"2023-02-20 03:37:33\",\n" +
                "  \"warn_type\" : \"T37W36\",\n" +
                "  \"pics\" : [ \"group2_M01/49/A8/rBCx2GPybdGAbDgAAATdVjwdzCs394.jpg\" ]\n" +
                "}, 悦秋小区-淞虹路715弄23号, null, -1, dap, 2023-02-20T03:37:34, dap, 2023-02-20T03:37:34, 5E5980A1DF3E4A86B01CDEDB5E4E8445, 8F0C3B701B994235AC07145D2774AE0A, null],1592323200,0)";
        String pattern_string = "timestamp[(](?<timestamp>.*)[)]";
        String pattern_eid = "^[0-9]*\\.+[0-9]+$";
        for (String item : tst_array
        ) {
            if (Pattern.compile(pattern_eid).matcher(item).matches()) {
                System.out.println(item);
            }
        }
        Pattern tst_pattern = Pattern.compile(pattern_string);
        Matcher tst_matcher = tst_pattern.matcher(tst_String);
        tst_matcher.find();
        System.out.println(tst_matcher.group("timestamp"));
    }
}
