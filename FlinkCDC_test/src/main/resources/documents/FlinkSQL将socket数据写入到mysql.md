# FlinkSQL将socket数据写入到mysql

——本章节主要演示从socket接收数据，通过滚动窗口每30秒运算一次窗口数据，然后将结果写入Mysql数据库

![img](FlinkSQL%E5%B0%86socket%E6%95%B0%E6%8D%AE%E5%86%99%E5%85%A5%E5%88%B0mysql.assets/94cad1c8a786c9173380f538d8fbbcc53ac75704.png)

（1）准备一个实体对象，消息对象

```java


package com.pojo;

import java.io.Serializable;

/**
\* Created by lj on 2022-07-05.
*/
public class WaterSensor implements Serializable {
private String id;
private long ts;
private int vc;

public WaterSensor(){

}

public WaterSensor(String id,long ts,int vc){
this.id = id;
this.ts = ts;
this.vc = vc;
}

public int getVc() {
return vc;
}

public void setVc(int vc) {
this.vc = vc;
}

public String getId() {
return id;
}

public void setId(String id) {
this.id = id;
}

public long getTs() {
return ts;
}

public void setTs(long ts) {
this.ts = ts;
}
}
```

（2）编写socket代码，模拟数据发送

```java
package com.producers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
\* Created by lj on 2022-07-05.
*/
public class Socket_Producer {
public static void main(String[] args) throws IOException {

try {
ServerSocket ss = new ServerSocket(9999);
System.out.println("启动 server ....");
Socket s = ss.accept();
BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
String response = "java,1,2";

//每 2s 发送一次消息
int i = 0;
Random r=new Random();
String[] lang = {"flink","spark","hadoop","hive","hbase","impala","presto","superset","nbi"};

while(true){
Thread.sleep(2000);
response= lang[r.nextInt(lang.length)] + "," + i + "," + i+"\n";
System.out.println(response);
try{
bw.write(response);
bw.flush();
i++;
}catch (Exception ex){
System.out.println(ex.getMessage());
}

}
} catch (IOException | InterruptedException e) {
e.printStackTrace();
}
}
}
```

（3）从socket端接收数据，并设置30秒触发执行一次窗口运算

```java
package com.examples;

import com.pojo.WaterSensor;
import com.sinks.RetractStream_Mysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
\* Created by lj on 2022-07-06.
*/

public class Flink_Group_Window_Tumble_Sink_Mysql {
public static void main(String[] args) throws Exception {

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1", 9999,"\n");
SingleOutputStreamOperator<WaterSensor> waterDS = streamSource.map(new MapFunction<String, WaterSensor>() {
@Override
public WaterSensor map(String s) throws Exception {
String[] split = s.split(",");
return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
}
});

// 将流转化为表
Table table = tableEnv.fromDataStream(waterDS,
$("id"),
$("ts"),
$("vc"),
$("pt").proctime());

tableEnv.createTemporaryView("EventTable", table);

Table result = tableEnv.sqlQuery(
"SELECT " +
"id, " + //window_start, window_end,
"COUNT(ts) ,SUM(ts)" +
"FROM TABLE( " +
"TUMBLE( TABLE EventTable , " +
"DESCRIPTOR(pt), " +
"INTERVAL '30' SECOND)) " +
"GROUP BY id , window_start, window_end"
);

tableEnv.toRetractStream(result, Row.class).addSink(new RetractStream_Mysql());
env.execute();
}
}

```

（4）定义一个写入到mysql的sink

```java


package com.sinks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
\* Created by lj on 2022-07-06.
*/
public class RetractStream_Mysql extends RichSinkFunction<Tuple2<Boolean, Row>> {

private static final long serialVersionUID = -4443175430371919407L;
PreparedStatement ps;
private Connection connection;

/**
\* open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
*
\* @param parameters
\* @throws Exception
*/
@Override
public void open(Configuration parameters) throws Exception {
super.open(parameters);
connection = getConnection();
}

@Override
public void close() throws Exception {
super.close();
//关闭连接和释放资源
if (connection != null) {
connection.close();
}
if (ps != null) {
ps.close();
}
}

/**
\* 每条数据的插入都要调用一次 invoke() 方法
*
\* @param context
\* @throws Exception
*/
@Override
public void invoke(Tuple2<Boolean, Row> userPvEntity, Context context) throws Exception {
String sql = "INSERT INTO flinkcomponent(componentname,componentcount,componentsum) VALUES(?,?,?);";
ps = this.connection.prepareStatement(sql);

ps.setString(1,userPvEntity.f1.getField(0).toString());
ps.setInt(2, Integer.parseInt(userPvEntity.f1.getField(1).toString()));
ps.setInt(3, Integer.parseInt(userPvEntity.f1.getField(2).toString()));
ps.executeUpdate();
}

private static Connection getConnection() {
Connection con = null;
try {
Class.forName("com.mysql.jdbc.Driver");
con = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb?useUnicode=true&characterEncoding=UTF-8&useSSL=false","root","root");
} catch (Exception e) {
System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
}
return con;
}
}
```





（5）效果演示，每30秒往数据库写一次数据

![img](FlinkSQL%E5%B0%86socket%E6%95%B0%E6%8D%AE%E5%86%99%E5%85%A5%E5%88%B0mysql.assets/0e2442a7d933c89518e9e948c0d5bffa830200ba.jpeg)