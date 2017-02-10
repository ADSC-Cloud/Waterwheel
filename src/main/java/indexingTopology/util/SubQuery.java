package indexingTopology.util;

import java.io.Serializable;

public class SubQuery <T extends Number> implements Serializable {

    private long queryId;

    private T leftKey;

    private T rightKey;

    private String fileName;

    private Long startTimestamp;

    private Long endTimestamp;

    private DataTuplePredicate predicate;


    public SubQuery(long queryId, T leftKey, T rightKey
            , String fileName, Long startTimestamp, Long endTimestamp, DataTuplePredicate predicate) {
        this.queryId = queryId;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.fileName = fileName;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.predicate = predicate;
    }

    public SubQuery(long queryId, T leftKey, T rightKey
            , String fileName, Long startTimestamp, Long endTimestamp) {
        this(queryId, leftKey, rightKey, fileName, startTimestamp, endTimestamp, null);
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

    public String getFileName() {
        return fileName;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public DataTuplePredicate getPredicate() {
        return predicate;
    }

}
