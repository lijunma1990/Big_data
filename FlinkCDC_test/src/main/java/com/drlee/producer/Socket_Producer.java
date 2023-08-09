package com.drlee.producer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * created by Drlee on 2023-03-16
 * 编写socket代码，模拟数据发送
 */
public class Socket_Producer {
    public static void main(String[] args) throws IOException {
        try {
            ServerSocket socket_server_tst = new ServerSocket(9999);
            System.out.println("启动 server ....");
            Socket str = socket_server_tst.accept();
            OutputStreamWriter str_stream = new OutputStreamWriter(str.getOutputStream());
            BufferedWriter bufferedWriter_tst = new BufferedWriter(str_stream);
            String response = "java,1,2";

            //每2s发送一次消息
            int i = 0;
            Random r = new Random();
            String[] lang = {"flink", "spark", "hadoop", "hive", "hbase", "impala", "presto", "superset", "nbi"};

            while (true) {
                Thread.sleep(2000);
                response = lang[r.nextInt(lang.length)] + "," + i + "," + i + "n";
                System.out.println(response);
                try {
                    bufferedWriter_tst.write(response);
                    bufferedWriter_tst.flush();
                    i++;
                } catch (Exception ex) {
                    System.out.println(ex.getMessage());
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
