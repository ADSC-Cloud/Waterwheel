package indexingTopology.api.client;

import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.logics.DataTupleEquivalentPredicateHint;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;

/**
 * Created by acelzj on 6/19/17.
 */
public class NetworkTemporalQueryRequest <T extends Number>  extends IClientRequest {

    final public T destIpLowerBound;
    final public T destIpUpperBound;
    final public long startTime;
    final public long endTime;
    final public DataTuplePredicate predicate;
    final public Aggregator aggregator;
    final public DataTupleSorter sorter;
    final public DataTupleEquivalentPredicateHint equivalentPredicate;
    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                   Aggregator aggregator, DataTupleSorter sorter, DataTupleEquivalentPredicateHint equivalentPredicate) {
        this.destIpLowerBound = destIpLowerBound;
        this.destIpUpperBound = destIpUpperBound;
        this.startTime = startTime;
        this.endTime = endTime;
        this.predicate = predicate;
        this.aggregator = aggregator;
        this.sorter = sorter;
        this.equivalentPredicate = equivalentPredicate;
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                   Aggregator aggregator, DataTupleSorter sorter) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, predicate, aggregator, sorter, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                   Aggregator aggregator) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, predicate, aggregator, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, predicate, null, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, Aggregator aggregator) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, null, aggregator, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, null, null, null);
    }
}
