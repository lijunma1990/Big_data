package dx.ict.test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeStamp_test {
    public static void main(String[] args) {
        long tt =  0L;
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        final LocalDateTime localDateTime = new Timestamp(tt).toLocalDateTime();
        String dateString = dateFormat.format(localDateTime);
        System.out.println(dateString);
    }
}
