package indexingTopology.util;

import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;

import java.io.Serializable;

public class SubQuery <T extends Number> implements Serializable {

    final public long queryId;

    final public T leftKey;

    final public T rightKey;

    final public Long startTimestamp;

    final public Long endTimestamp;

    final public DataTuplePredicate predicate;

    final public Aggregator aggregator;

    final public DataTupleSorter sorter;

    final public DataTupleEquivalentPredicateHint equivalentPredicate;

    public SubQuery(long queryId, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp,
                    DataTuplePredicate predicate, Aggregator aggregator, DataTupleSorter sorter,
                    DataTupleEquivalentPredicateHint equivalentPredicate) {
        this.queryId = queryId;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.predicate = predicate;
        this.aggregator = aggregator;
        this.sorter = sorter;
        this.equivalentPredicate = equivalentPredicate;
    }

    public SubQuery(long queryId, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp,
                    DataTuplePredicate predicate, Aggregator aggregator, DataTupleSorter sorter) {
        this(queryId, leftKey, rightKey, startTimestamp, endTimestamp, predicate, aggregator, sorter, null);
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
