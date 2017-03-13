package indexingTopology.util;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.data.DataTuple;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by robert on 9/2/17.
 */
public class Query <T extends Number> implements Serializable {
    public T leftKey;
    public T rightKey;
    public Long startTimestamp;
    public Long endTimestamp;
    public long id;
    public DataTuplePredicate predicate;
    public Aggregator aggregator;

    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp) {
        this(id, leftKey, rightKey, startTimestamp, endTimestamp, null, null);
    }

    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp, DataTuplePredicate predicate,
                 Aggregator aggregator) {
        this.id = id;
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.predicate = predicate;
        this.aggregator = aggregator;
    }

}
