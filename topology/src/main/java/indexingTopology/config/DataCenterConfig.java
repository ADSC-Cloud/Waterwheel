package indexingTopology.config;

/**
 * Created by billlin on 2017/11/25
 */
public class DataCenterConfig {
    /** ES连接ip地址**/
    private static String esIp;

    /** ES 端口 **/
    private static String esPort;

    /** pvuv订阅主题 **/
    private static String pvuvTopic = "test";

    /** ZK root节点**/
    private static String zkRoot = "";

    /** ZK 地址端口 **/
    private static String brokerZkStr = "localhost:2181";

    public static String getBrokerZkStr() {
        return brokerZkStr;
    }

    public static String getZkRoot(){
        return zkRoot;
    }

    public static String getPvuvTopic(){
        return pvuvTopic;
    }

}
