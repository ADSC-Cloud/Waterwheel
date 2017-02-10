package indexingTopology.util;

import java.io.Serializable;

public class SubQuery <T extends Number> implements Serializable {

    private long queryId;

    private T leftKey;

    private T rightKey;

    private Long startTimestamp;

    private Long endTimestamp;


    public SubQuery(long queryId, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp) {
        this.queryId = queryId;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public long getQueryId() {
        return queryId;
    }

    public T getLeftKey() {
        return leftKey;
    }

    public T getRightKey() {
        return rightKey;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

}
