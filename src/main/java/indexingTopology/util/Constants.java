package indexingTopology.util;

/**
 * Created by parijatmazumdar on 24/10/15.
 */
public enum Constants {
    HDFS_CORE_SITE("core_site"),
    HDFS_HDFS_SITE("hdfs_site");

    public final String str;
    Constants(String str) {
        this.str=str;
    }
}
