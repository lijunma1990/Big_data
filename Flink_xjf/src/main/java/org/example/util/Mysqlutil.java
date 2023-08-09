package org.example.util;

import java.io.Serializable;
import java.text.MessageFormat;

public class Mysqlutil implements Serializable {

    public static void main(String[] args) {
        String TableName = "xxx";
        String sql = String.format("select * from %s where id > 0", TableName);
        String sql1 = String.format("select * from {0} where id > 0", TableName);
        String sql2 = MessageFormat.format("select * from {0} where id > 0", TableName);

        System.out.println(sql);
        System.out.println(sql1);
        System.out.println(sql2);
    }



}
