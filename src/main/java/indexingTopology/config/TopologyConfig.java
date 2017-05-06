package indexingTopology.config;

/**
 * Created by acelzj on 7/21/16.
 */
public class TopologyConfig {
    public static final double REBUILD_TEMPLATE_THRESHOLD = 10.0;

    public static final String HDFS_HOST = "hdfs://192.168.0.237:54310/";

    public static final int NUMBER_TUPLES_OF_A_CHUNK = 600000 * 2;
//    public static final int NUMBER_TUPLES_OF_A_CHUNK = 200000;
    public static final int CACHE_SIZE = 10000;

    public static final int TASK_QUEUE_CAPACITY = 10000;

    public static int NUMBER_OF_INTERVALS = 100000;

    public static final int BTREE_ORDER = 64;

    public static final double LOAD_BALANCE_THRESHOLD = 0.2;

    public static boolean HDFSFlag = false;

    public static String dataDir = "/Users/Robert/Documents/code/data";

    public static String dataFileDir = "/home/wangli/kingbase-topology";
//    public static String dataFileDir = "/home/lzj/taxi-trajectory";
//    public static String dataFileDir = "/home/lzj/dataset/20150430_processed.txt";
//    public static String dataFileDir = "/home/acelzj/Downloads/taxi-trajectory";
//    public static String dataFileDir = "/home/acelzj/Downloads/dataset/20150430.txt";

    public static String logDir = "/logs";

    public static double SKEWNESS_DETECTION_THRESHOLD = 0.3;

//    public static final int PENDING_QUEUE_CAPACITY = 1024;
    public static final int PENDING_QUEUE_CAPACITY = 600001 * 2;

    public static final int MAX_PENDING = 1000;
    public static final int EMIT_NUM = 50;

    public static final int OFFSET_LENGTH = 4;

    public static final int CHUNK_SIZE = 58000000 / 8;
//    public static final int CHUNK_SIZE = 6 * 1024 * 1024;


    public static final String ZOOKEEPER_HOST = "192.168.0.207";

    public static final String HBASE_CONF_DIR = "/home/hduser/hbase-1.2.4/conf";

    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "yy01-Precision-T1650,yy09-Precision-T1650,yy10-ADSC,yy11-T5810,yy06-Precision-T1650,yy07-Precision-T1650,yy08-Precision-T1650,yy02-ubuntu,yy03-Precision-T1650,yy04-Precision-T1650,yy05-Precision-T1650";

    public static final int HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = 2222;

    public static final String HBASE_MASTER = "192.168.0.237:60000";

    public static final boolean SHUFFLE_GROUPING_FLAG = false;

    public static final int AVERAGE_STRING_LENGTH = 21;

    public static final boolean TASK_QUEUE_MODEL = false;
}
