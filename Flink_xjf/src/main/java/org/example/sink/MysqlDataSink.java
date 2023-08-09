package org.example.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class MysqlDataSink<T> {
    private DataStreamSource<T> streamSource;
    private String URL = null;
    private String UserName = null;
    private String PassWord = null;
    private String DRIVERNAME = "com.mysql.cj.jdbc.Driver";

    public MysqlDataSink(DataStreamSource<T> streamSource) {
        this.streamSource = streamSource;
    }

    public MysqlDataSink(DataStreamSource<T> streamSource, String URL, String userName, String passWord) {
        this.streamSource = streamSource;
        this.URL = URL;
        UserName = userName;
        PassWord = passWord;
    }

    public void toSink(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder) {
        streamSource.addSink(JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(this.URL)
                        .withDriverName(DRIVERNAME)
                        .withUsername(UserName)
                        .withPassword(PassWord)
                        .build()));
    }
}


