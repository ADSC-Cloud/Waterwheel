package indexingTopology.config;

import java.io.Serializable;

/**
 * Created by acelzj on 7/21/16.
 */
public class TopologyConfig implements Serializable {
    public final double REBUILD_TEMPLATE_THRESHOLD = 10.0;

//    public static final String HDFS_HOST = "hdfs://192.168.0.237:54310/";

    public final String HDFS_HOST = "hdfs://10.21.25.10:54310/";
//    public static final String HDFS_HOST = "hdfs://10.21.25.13:54310/";

    /**
     * Used for maintaining persistent meta logs in HDFS. In case reconstruction
     * is needed such as when the system crashed, meta data stored in {@link TopologyConfig#HDFS_META_LOG_PATH}
     * is used for reconstruction.
     */
    public boolean RECONSTRUCT_SCHEMA = true;
    public final String HDFS_META_LOG_PATH = "hdfs://localhost:9000/user/john/metaLog.txt";
    public final String HDFS_HOST_LOCAL = "hdfs://localhost:9000/";

    public final int NUMBER_TUPLES_OF_A_CHUNK = 600000;

    public final int CACHE_SIZE = 200;

    public final int TASK_QUEUE_CAPACITY = 10000;

    public int NUMBER_OF_INTERVALS = 100000;

    public final int BTREE_ORDER = 64;

    public final double LOAD_BALANCE_THRESHOLD = 0.2;

    public boolean HDFSFlag = false;

    public boolean HybridStorage = false;

    public boolean HdfsTaskLocality = false;

    public boolean ChunkOrientedCaching = true;

//    public static String dataDir = "/Users/Robert/Documents/data";
//    public String dataDir = "/home/robert/data";

    public String dataDir = "/home/robert/data";

//    public static String dataDir = "./";

    public String dataFileDir = "/home/robert/data/t";
//    public static String dataFileDir = "/home/lzj/dataset/20150430_processed.txt";
//    public static String dataFileDir = "/home/acelzj/Downloads/DPI/20150430_processed.txt";
//    public static String dataFileDir = "/home/acelzj/Downloads/DPI/20150430.txt";

    public String logDir = "/logs";

    public double SKEWNESS_DETECTION_THRESHOLD = 0.3;

//    public static final int PENDING_QUEUE_CAPACITY = 1024;
    public final int PENDING_QUEUE_CAPACITY = 600001 * 2;

    public final int MAX_PENDING = 100000;
    public final int EMIT_NUM = 500;

    public final int OFFSET_LENGTH = 4;

    public int CHUNK_SIZE = 58000000 / 4;
//    public static final int CHUNK_SIZE = 58000000 / 16;
//    public static final int CHUNK_SIZE = 58000000 / 8;
//    public static final int CHUNK_SIZE = 58000000 / 2;
//    public static final int CHUNK_SIZE = 58000000;
//    public static final int CHUNK_SIZE = 6 * 1024 * 1024;


    public static final String ZOOKEEPER_HOST = "192.168.0.207";
//    public static final String ZOOKEEPER_HOST = "10.21.25.14";

    public final String HBASE_CONF_DIR = "/home/hduser/hbase-1.2.4/conf";

    public final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "yy01-Precision-T1650,yy09-Precision-T1650,yy10-ADSC,yy11-T5810,yy06-Precision-T1650,yy07-Precision-T1650,yy08-Precision-T1650,yy02-ubuntu,yy03-Precision-T1650,yy04-Precision-T1650,yy05-Precision-T1650";

    public final int HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = 2222;

    public final String HBASE_MASTER = "192.168.0.237:60000";

    public final boolean SHUFFLE_GROUPING_FLAG = false;

    public final int AVERAGE_STRING_LENGTH = 21;

    public final boolean TASK_QUEUE_MODEL = false;

    // parallelism configuration
    public int CHUNK_SCANNER_PER_NODE = 2;

    public int INSERTION_SERVER_PER_NODE = 2;
}
