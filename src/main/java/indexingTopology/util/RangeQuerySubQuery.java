package indexingTopology.util;

import java.io.Serializable;

public class RangeQuerySubQuery implements Serializable {

    private long queryId;

    private Double leftKey;

    private Double rightKey;

    private String fileName;

    private Long startTimestamp;

    private Long endTimestamp;


    public RangeQuerySubQuery(long queryId,  Double leftKey, Double rightKey
            , String fileName, Long startTimestamp, Long endTimestamp) {
        this.queryId = queryId;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.fileName = fileName;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public long getQueryId() {
        return queryId;
    }

    public Double getlefKey() {
        return leftKey;
    }

    public Double getRightKey() {
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

}
