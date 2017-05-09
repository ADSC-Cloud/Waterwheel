package indexingTopology.util;

import indexingTopology.aggregator.Aggregator;

import java.io.Serializable;

public class SubQuery <T extends Number> implements Serializable {

    public long queryId;

    public T leftKey;

    public T rightKey;

    public Long startTimestamp;

    public Long endTimestamp;

    public DataTuplePredicate predicate;

    public Aggregator aggregator;

    public DataTupleSorter sorter;

    public SubQuery(long queryId, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp,
                    DataTuplePredicate predicate, Aggregator aggregator, DataTupleSorter sorter) {
        this.queryId = queryId;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.predicate = predicate;
        this.aggregator = aggregator;
        this.sorter = sorter;
    }

    public SubQuery(long queryId, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp,
                    DataTuplePredicate predicate, Aggregator aggregator) {
        this(queryId, leftKey, rightKey, startTimestamp, endTimestamp, predicate, aggregator, null);
    }

    public SubQuery(long queryId, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp,
                    DataTuplePredicate predicate) {
        this(queryId, leftKey, rightKey, startTimestamp, endTimestamp, predicate, null, null);
    }


    public SubQuery(long queryId, T leftKey, T rightKey
            ,Long startTimestamp, Long endTimestamp) {
        this(queryId, leftKey, rightKey, startTimestamp, endTimestamp, null, null);
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

    public DataTuplePredicate getPredicate() {
        return predicate;
    }

    public Aggregator getAggregator() {
        return aggregator;
    }

    public String toString() {
        String str = "Query: ";
        str += String.format("key: [%s, %s], time: [%d, %d], predicate: %s, aggregator: %s", leftKey, rightKey,
                startTimestamp, endTimestamp, predicate, aggregator);
        return str;
    }

}
