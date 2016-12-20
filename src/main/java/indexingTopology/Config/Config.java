package indexingTopology.Config;

/**
 * Created by acelzj on 7/21/16.
 */
public class Config {
    public static final double REBUILD_TEMPLATE_PERCENTAGE = 20;

    public static final double TEMPLATE_OVERFLOW_PERCENTAGE = 1;

    public static final String HDFS_HOST = "hdfs://localhost:54310/";

    public static final int TEMPLATE_SIZE = 64000 * 4;
//    public static final int TEMPLATE_SIZE = 64000 ;

    public static final int NUMBER_TUPLES_OF_A_CHUNK = 20000;
//    public static final int NUMBER_TUPLES_OF_A_CHUNK = 2000;

    public static final double KER_RANGE_COVERAGE = 0.2;

    public static final int CACHE_SIZE = 10;

    public static final int TASK_QUEUE_CAPACITY = 10000;

    public static final int NUMBER_OF_INTERVALS = 10;

    public static final int BTREE_OREDER = 4;

    public static final double LOAD_BALANCE_THRESHOLD = 0.2;
}
