package indexingTopology.util;

/**
 * Created by parijatmazumdar on 24/10/15.
 */
public enum Constants {
    HDFS_CORE_SITE("core_site"),
    HDFS_HDFS_SITE("hdfs_site"),
    TIME_SERIALIZATION_WRITE("time_serialization_write"),
    TIME_INSERTION("time_insertion"),
    TIME_LEAF_FIND("time_leaf_find"),
    TIME_LEAF_INSERTION("time_leaf_insertion"),
    TIME_SPLIT("time_split"),
    TIME_CHUNK_START("time_chunk_start"),
    TIME_TOTAL("time_total");

    public final String str;
    Constants(String str) {
        this.str=str;
    }
}
