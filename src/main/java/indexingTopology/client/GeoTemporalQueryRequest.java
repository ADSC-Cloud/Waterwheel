package indexingTopology.client;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.util.DataTupleEquivalentPredicateHint;
import indexingTopology.util.DataTuplePredicate;
import indexingTopology.util.DataTupleSorter;

/**
 * Created by robert on 3/3/17.
 */
public class GeoTemporalQueryRequest<T extends Number>  extends IClientRequest {
    final public T x1;
    final public T x2;
    final public T y1;
    final public T y2;
    final public long startTime;
    final public long endTime;
    final public DataTuplePredicate predicate;
    final public Aggregator aggregator;
    final public DataTupleSorter sorter;
    final public DataTupleEquivalentPredicateHint equivalentPredicate;
    public GeoTemporalQueryRequest(T x1, T x2, T y1, T y2, long startTime, long endTime, DataTuplePredicate predicate,
                                   Aggregator aggregator, DataTupleSorter sorter, DataTupleEquivalentPredicateHint equivalentPredicate) {
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
        this.startTime = startTime;
        this.endTime = endTime;
        this.predicate = predicate;
        this.aggregator = aggregator;
        this.sorter = sorter;
        this.equivalentPredicate = equivalentPredicate;
    }

    public GeoTemporalQueryRequest(T x1, T x2, T y1, T y2, long startTime, long endTime, DataTuplePredicate predicate,
                                   Aggregator aggregator, DataTupleSorter sorter) {
        this(x1, x2, y1, y2, startTime, endTime, predicate, aggregator, sorter, null);
    }

    public GeoTemporalQueryRequest(T x1, T x2, T y1, T y2, long startTime, long endTime, DataTuplePredicate predicate) {
        this(x1, x2, y1, y2, startTime, endTime, predicate, null, null);
    }

    public GeoTemporalQueryRequest(T x1, T x2, T y1, T y2, long startTime, long endTime, Aggregator aggregator) {
        this(x1, x2, y1, y2, startTime, endTime, null, aggregator, null);
    }

    public GeoTemporalQueryRequest(T x1, T x2, T y1, T y2, long startTime, long endTime) {
        this(x1, x2, y1, y2, startTime, endTime, null, null, null);
    }
}
