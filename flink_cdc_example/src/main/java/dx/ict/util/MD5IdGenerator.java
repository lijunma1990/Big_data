package dx.ict.util;

/**
 * @Author 李二白
 * @Description TODO
 * @Date 2023/6/12 14:30
 * @Version 1.0
 */
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public class MD5IdGenerator {

    public static String generateUniqueId() {
        // 获取当前时间戳（精确到毫秒）
        long timestamp = System.currentTimeMillis();

        // 生成一个UUID作为随机数
        String uuid = UUID.randomUUID().toString();

        // 将时间戳和UUID拼接成字符串
        String input = timestamp + uuid;

        // 计算MD5散列值
        String hash = getMd5Hash(input);

        // 返回结果
        return hash;
    }

    private static String getMd5Hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(input.getBytes());

            // 将字节数组转化为字符串
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            String id = generateUniqueId();
            System.out.println(id);
        }
    }
}
