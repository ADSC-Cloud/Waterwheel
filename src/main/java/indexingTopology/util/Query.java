package indexingTopology.util;

import java.io.Serializable;

/**
 * Created by robert on 9/2/17.
 */
public class Query <T extends Number> implements Serializable {
    public T leftKey;
    public T rightKey;
    public Long startTimestamp;
    public Long endTimestamp;
    public long id;
    public Query(T leftKey, T rightKey, Long startTimestamp, Long endTimestamp) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }
    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp) {
        this.id = id;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }
}
