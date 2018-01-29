package indexingTopology.config;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class TopologyConfig implements Serializable {
    public final double REBUILD_TEMPLATE_THRESHOLD = 10.0;



    public String dataChunkDir = "/home/hadoop/dataDir";

    public String metadataDir = "/home/hadoop/dataDir";


    public String HDFS_HOST = "hdfs://192.168.0.237:54310/";

    public final int CACHE_SIZE = 200;

    public final int TASK_QUEUE_CAPACITY = 10000;

    public int NUMBER_OF_INTERVALS = 1024;

    public final int BTREE_ORDER = 64;

    public final double LOAD_BALANCE_THRESHOLD = 0.2;

    public boolean HDFSFlag = false;

    public boolean HybridStorage = false;

    public boolean HdfsTaskLocality = true;

    public boolean ChunkOrientedCaching = false;

    final static public int DataChunkHeaderSectionSize = 1024;

    final static public boolean compression = true;

    public String logDir = "/logs";

    public double SKEWNESS_DETECTION_THRESHOLD = 0.3;

//    public static final int PENDING_QUEUE_CAPACITY = 1024;
    public final int PENDING_QUEUE_CAPACITY = 600001 * 2;

    public final int MAX_PENDING = 100000;
    public final int EMIT_NUM = 500;

    public final int OFFSET_LENGTH = 4;

    public int CHUNK_SIZE = 58000000 / 4;

    // parallelism configuration
    public int CHUNK_SCANNER_PER_NODE = 4;

    public int INSERTION_SERVER_PER_NODE = 2;

    public int DISPATCHER_PER_NODE = 1;

    public int removeIntervalHours = 6; // Delete the time interval for old data

    public int previousTime = 24; // Define the time for old data

    public String topic = "gpis";

    public ArrayList<String> ZKHost = new ArrayList<String>() {{
        add("localhost:2181");
    }};

    public ArrayList<String> kafkaHost = new ArrayList<String>() {{
        add("localhost:9092");
    }};
//    public ArrayList<String> ZKHost = new ArrayList<String>();

//    public List<String> kafkaHost = new ArrayList<String>();

    public static final String ZOOKEEPER_HOST = "192.168.0.207";

    public final boolean SHUFFLE_GROUPING_FLAG = false;

    public final boolean TASK_QUEUE_MODEL = false;

    static public final int StaticRequestTimeIntervalInSeconds = 5;

    /**
     * Used for maintaining persistent meta logs in HDFS. In case reconstruction
     * is needed such as when the system crashed, meta data stored in {@link TopologyConfig#HDFS_META_LOG_PATH}
     * is used for reconstruction.
     */
    public boolean RECONSTRUCT_SCHEMA = true;
    public final String HDFS_META_LOG_PATH = "hdfs://localhost:9000/user/john/metaLog.txt";
    public final String HDFS_HOST_LOCAL = "hdfs://localhost:9000/";

    /**
     * configures that should have not been put in this class
     */
    public final int AVERAGE_STRING_LENGTH = 21;
    public final String HBASE_CONF_DIR = "/home/hduser/hbase-1.2.4/conf";
    public final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "yy01-Precision-T1650,yy09-Precision-T1650,yy10-ADSC,yy11-T5810,yy06-Precision-T1650,yy07-Precision-T1650,yy08-Precision-T1650,yy02-ubuntu,yy03-Precision-T1650,yy04-Precision-T1650,yy05-Precision-T1650";
    public final int HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = 2222;
    public final String HBASE_MASTER = "192.168.0.237:60000";

    public String getCriticalSettings() {
        String ret = "";
        ret += String.format("dataChunkDir: %s\n", dataChunkDir);
        ret += String.format("metadataDir: %s\n", metadataDir);
        ret += String.format("HDFS host: %s\n", HDFS_HOST);
        ret += String.format("removeIntervalHours: %d\n", removeIntervalHours);
        ret += String.format("previousTime: %d\n", previousTime);
        ret += String.format("HDFS: %s\n", HDFSFlag);
        ret += String.format("topic: %s\n", topic);
        ret += String.format("ZKHost: %s\n", ZKHost);
        ret += String.format("kafkaHost: %s\n", kafkaHost);
        return ret;
    }

    public void override(String filePath) {
        File file = new File(filePath);
        try {
            Yaml yaml = new Yaml();
            InputStream ios = new FileInputStream(file);
            Map<String, Object> result = (Map< String, Object>) yaml.load(ios);

            if (result.containsKey("storage.datachunk.folder"))
                this.dataChunkDir = (String)result.get("storage.datachunk.folder");

            if (result.containsKey("metadata.local.storage.path"))
                this.metadataDir = (String)result.get("metadata.local.storage.path");

            if (result.containsKey("hdfs.host"))
                this.HDFS_HOST = (String)result.get("hdfs.host");

            if (result.containsKey("storage.file.system")) {
                this.HDFSFlag = result.get("storage.file.system").equals("hdfs");
            }

//            if (result.containsKey("remove.interval.hours")){
//                this.removeIntervalHours = (Integer)result.get("remove.interval.hours");
//            }

            if (result.containsKey("old.data.previous.time")) {
                this.previousTime = (Integer)result.get("old.data.previous.time");
            }

            if (result.containsKey("remove.interval.hours")) {
                this.removeIntervalHours = (Integer)result.get("remove.interval.hours");
            }

            if (result.containsKey("topic")) {
                this.topic = (String) result.get("topic");
            }

            if (result.containsKey("ZKhost")) {
                ArrayList<String> Zkhost = (ArrayList<String>)(result.get("ZKhost"));
                this.ZKHost.remove(0);
                System.out.println("ZKhost.size:" + Zkhost.size());
                for(int i = 0; i < Zkhost.size(); i++){
                    this.ZKHost.add(Zkhost.get(i));
                }
            }

            if (result.containsKey("kafkaHost")) {
                ArrayList<String> kafkaHost = (ArrayList<String>)(result.get("kafkaHost"));
                this.kafkaHost.remove(0);
                System.out.println("kafkaHost.size:" + kafkaHost.size());
                for(int i = 0; i < kafkaHost.size(); i++){
                    this.kafkaHost.add(kafkaHost.get(i));
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("The configure file " + filePath + " is not found. Use default conf instead.");
        }
    }

    public static void main(String[] args) {
        TopologyConfig config = new TopologyConfig();
        System.out.println(config.getCriticalSettings());
    }
}
