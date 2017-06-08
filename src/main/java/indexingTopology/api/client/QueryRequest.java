package indexingTopology.api.client;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;

/**
 * Created by robert on 3/3/17.
 */
public class QueryRequest<T extends Number>  extends IClientRequest {
    final public T low;
    final public T high;
    final public long startTime;
    final public long endTime;
    final public DataTuplePredicate predicate;
    final public Aggregator aggregator;
    final public DataTupleSorter sorter;
    final public DataTupleEquivalentPredicateHint equivalentPredicate;
    public QueryRequest(T low, T high, long startTime, long endTime, DataTuplePredicate predicate,
                        Aggregator aggregator, DataTupleSorter sorter, DataTupleEquivalentPredicateHint equivalentPredicate) {
        this.low = low;
        this.high = high;
        this.startTime = startTime;
        this.endTime = endTime;
        this.predicate = predicate;
        this.aggregator = aggregator;
        this.sorter = sorter;
        this.equivalentPredicate = equivalentPredicate;
    }

    public QueryRequest(T low, T high, long startTime, long endTime, DataTuplePredicate predicate, Aggregator aggregator,
                        DataTupleSorter sorter) {
        this(low, high, startTime, endTime, predicate, aggregator, sorter, null);
    }

    public QueryRequest(T low, T high, long startTime, long endTime, DataTuplePredicate predicate, Aggregator aggregator) {
        this(low, high, startTime, endTime, predicate, aggregator, null);
    }

    public QueryRequest(T low, T high, long startTime, long endTime, DataTuplePredicate predicate) {
        this(low, high, startTime, endTime, predicate, null);
    }

    public QueryRequest(T low, T high, long startTime, long endTime, Aggregator aggregator) {
        this(low, high, startTime, endTime, null, aggregator);
    }

    public QueryRequest(T low, T high, long startTime, long endTime) {
        this(low, high, startTime, endTime, null, null);
    }
}
