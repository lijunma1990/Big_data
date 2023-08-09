package dx.ict.test;

public class DeserializationSchema_test {
    public static void main(String[] args) {
/**        {
 "before":null,
 "after":{
 "province_id":3, "province_name":"山东", "region_name":"华北"
 },
 "source":{
 "version":"1.5.4.Final", "connector":"mysql", "name":"mysql_binlog_source", "ts_ms":0, "snapshot":
 "false", "db":"fwb_test", "sequence":null, "table":"dim_province", "server_id":0, "gtid":null, "file":
 "", "pos":0, "row":0, "thread":null, "query":null
 },
 "op":"r",
 "ts_ms":1679921159154,
 "transaction":null
 }
 */
        String tet_string = "{\"before\":null,\"after\":{\"province_id\":9,\"province_name\":\"山东\",\"region_name\":\"华北\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":0,\"snapshot\":\"false\",\"db\":\"fwb_test\",\"sequence\":null,\"table\":\"dim_province\",\"server_id\":0,\"gtid\":null,\"file\":\"\",\"pos\":0,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1679965568286,\"transaction\":null}\n";
    }
}
