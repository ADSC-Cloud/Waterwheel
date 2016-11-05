package indexingTopology.Config;

/**
 * Created by acelzj on 7/21/16.
 */
public class Config {
    public static final double REBUILD_TEMPLATE_PERCENTAGE = 20;

    public static final double TEMPLATE_OVERFLOW_PERCENTAGE = 1;

    public static final String HDFS_HOST = "hdfs://localhost:54310/";

    public static final int TEMPLATE_SIZE = 64000;

    public static final int NUMBER_TUPLES_OF_A_CHUNK = 810000;

    public static final int LEAVE_NODE_IN_BYTES = 1000;
}
