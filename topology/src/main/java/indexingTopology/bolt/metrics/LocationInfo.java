package indexingTopology.bolt.metrics;

import java.io.Serializable;

/**
 * Created by robert on 22/5/17.
 */
public class LocationInfo implements Serializable {
    public enum Type {Ingestion, Query
    }

    public String location;
    public int taskId;
    public Type type;
    public LocationInfo(Type type, int taskId, String location) {
        this.type = type;
        this.taskId = taskId;
        this.location = location;

    }
}
