package indexingTopology.util;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.data.DataTuple;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by robert on 9/2/17.
 */
public class Query <T extends Number> extends SubQuery<T> {


    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp) {
        super(id, leftKey, rightKey, startTimestamp, endTimestamp, null, null);
    }

    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp, DataTuplePredicate predicate,
                 Aggregator aggregator) {
        super(id, leftKey, rightKey, startTimestamp, endTimestamp, predicate, aggregator, null);
    }

    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp, DataTuplePredicate predicate,
                 Aggregator aggregator, DataTupleSorter sorter) {
        super(id, leftKey, rightKey, startTimestamp, endTimestamp, predicate, aggregator, sorter);
    }

    public Query(long id, T leftKey, T rightKey, Long startTimestamp, Long endTimestamp, DataTuplePredicate predicate,
                 Aggregator aggregator, DataTupleSorter sorter, DataTupleEquivalentPredicate equivalentPredicate) {
        super(id, leftKey, rightKey, startTimestamp, endTimestamp, predicate, aggregator, sorter, equivalentPredicate);
    }

    public String toString() {
        String str = "Query: ";
        str += String.format("key: [%s, %s], time: [%d, %d], predicate: %s, aggregator: %s", leftKey, rightKey,
                startTimestamp, endTimestamp, predicate, aggregator);
        return str;
    }

}
