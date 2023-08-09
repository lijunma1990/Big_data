package flinkstu.source;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author KevenHe
 * @create 2022/2/13 22:15
 */
public class Env {
    //创建执行环境
    StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    //返回本地执行环境
    LocalStreamEnvironment localEnv =
            StreamExecutionEnvironment.createLocalEnvironment(1);
    //远程执行环境
    StreamExecutionEnvironment sEnv =
            StreamExecutionEnvironment.createRemoteEnvironment(
                    "jobmanage-hostname", 6123,"YOURPATH//WordCount.jar"
                    );


}
