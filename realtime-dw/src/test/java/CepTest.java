import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CepTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                " CREATE TABLE kafka_ad (                 " +
                        "      gid INT,                                       " +
                        "      adid   INT,                               " +
                        "      ts BIGINT,                               " +
                        "      eid STRING ,                          " +
                        "      rt as to_timestamp_ltz(ts,3)  ,"+
                        "      watermark for rt as rt "+
                        " ) WITH (                                                " +
                        "  'connector' = 'kafka',                          " +
                        "  'topic' = 'test-ad',                                " +
                        "  'properties.bootstrap.servers' = 'doitedu:9092',       " +
                        "  'properties.group.id' = 'testGroup',                   " +
                        "  'scan.startup.mode' = 'latest-offset',                      "  +
                        "  'format' = 'csv'                        "  +
                        " ) ");

        tableEnv.executeSql("select * from kafka_ad")/*.print()*/;

        tableEnv.executeSql("SELECT *\n" +
                "FROM kafka_ad\n" +
                "    MATCH_RECOGNIZE(\n" +
                "        PARTITION BY gid,adid\n" +
                "        ORDER BY rt\n" +
                "        MEASURES\n" +
                "            A.eid AS view_eid,\n" +
                "            A.ts AS view_ts,\n" +
                "            B.eid AS click_eid,\n" +
                "            B.ts AS click_ts  \n" +
                "        ONE ROW PER MATCH\n" +
                "        AFTER MATCH SKIP TO NEXT ROW \n" +
                "        PATTERN (A C* B)  \n" +
                "        DEFINE\n" +
                "            A AS A.eid='view',\n" +
                "            B AS B.eid='click',\n" +
                "            C AS NOT C.eid='view' AND  NOT C.eid='click' \n" +
                ") AS T").print();

        env.execute();
    }
}
