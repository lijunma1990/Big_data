package dx.ict.util;

import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;

import java.util.UUID;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/6/12 13:10
 * @Version 1.0
 */
public class HashIdGenerator {

    /*
    生成哈希ID
     * */
    public static String generateUniqueId() {
        // 获取当前时间戳（精确到毫秒）
        long timestamp = System.currentTimeMillis();

        // 生成一个UUID作为随机数
        String uuid = UUID.randomUUID().toString();

        // 将时间戳和UUID拼接成字符串
        String input = timestamp + uuid;

        // 计算哈希值
        int hash = input.hashCode();

        // 将哈希值转换为正数
        long positiveHash = Math.abs((long) hash);

        // 将正数哈希值转换为字符串，并返回结果
        return Long.toString(positiveHash);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            String id = generateUniqueId();
            System.out.println(id);
        }
    }
}
