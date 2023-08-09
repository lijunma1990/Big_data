package com.keven.mall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.keven.mall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author KevenHe
 * @create 2022/1/19 21:19
 */

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    //value:{"sinkTable":"dim_base_trademark","database":"mall-210325-flink","before":{"tm_name":"kevenhe","id":12},"after":{"tm_name":"kevenhe","id":12},"type":"update","tableName":"base_trademark"}
    //SQL£∫upsert into db.tn(id,tm_name) values('...','...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //ªÒ»°SQL”Ôæ‰
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable,
                    after);
            System.out.println(upsertSql);

            //‘§±‡“ÎSQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //÷¥––≤Â»Î≤Ÿ◊˜
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //data:{"tm_name":"kevenhe","id":12}
    //SQL£∫upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        //keySet.mkString(",");  =>  "id,tm_name"
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
