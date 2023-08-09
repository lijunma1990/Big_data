package org.example.util;

import org.apache.flink.api.java.utils.ParameterTool;
import java.io.IOException;
import java.util.HashMap;

public class GetParameterFromProperties {
    public static HashMap<String, String> getParameter(String propertiesFilePath) {
        String URL = null;
        String UserName = null;
        String PassWord = null;
        HashMap<String, String> stringStringMap = new HashMap<>();

        try {
            ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesFilePath);
            // 获取properties文件里的配置参数
            URL = parameterTool.get("URL");
            UserName = parameterTool.get("UserName");
            PassWord = parameterTool.get("PassWord");
            //把获取的配置参数添加到HashMap集合
            stringStringMap.put("url", URL);
            stringStringMap.put("userName", UserName);
            stringStringMap.put("passWord", PassWord);

            return stringStringMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
