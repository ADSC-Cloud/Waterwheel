package indexingTopology.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by acelzj on 12/3/16.
 */
public class SubQuery {

    private long queryId;

    private Double key;

    private String fileName;

    private Long startTimestamp;

    private Long endTimestamp;

    public SubQuery(long queryId, Double key, String fileName, Long startTimestamp, Long endTimestamp) {
        this.queryId = queryId;
        this.key = key;
        this.fileName = fileName;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public long getQueryId() {
        return queryId;
    }

    public Double getKey() {
        return key;
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
