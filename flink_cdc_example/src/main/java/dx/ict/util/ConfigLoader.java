package dx.ict.util;

import java.io.IOException;
import java.io.InputStream;

import java.util.Properties;



public class ConfigLoader {
    private static final Properties pros = new Properties();

    static {
        InputStream resourceAsStream = ConfigLoader.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            pros.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String get(String key) {
        return pros.getProperty(key);
    }

    public static String get(String key,String  defaultValue){
        String value = pros.getProperty(key);
        if ( value.length() == 0){
            value = defaultValue;
        }
        return value;
    }

    public static Integer getInt(String key){
        return Integer.parseInt(pros.getProperty(key));
    }

    public static Integer getInt(String key,String defaultValue){
        String value = pros.getProperty(key);
        if (value.length() == 0){
            value = defaultValue;
        }
        return Integer.parseInt(value);
    }
}
