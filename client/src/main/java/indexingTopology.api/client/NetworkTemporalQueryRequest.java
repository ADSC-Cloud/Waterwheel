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
    final public DataTuplePredicate postPredicate;
    final public Aggregator aggregator;
    final public DataTupleSorter sorter;
    final public DataTupleEquivalentPredicateHint equivalentPredicate;
    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                       DataTuplePredicate postPredicate, Aggregator aggregator, DataTupleSorter sorter, DataTupleEquivalentPredicateHint equivalentPredicate) {
        this.destIpLowerBound = destIpLowerBound;
        this.destIpUpperBound = destIpUpperBound;
        this.startTime = startTime;
        this.endTime = endTime;
        this.predicate = predicate;
        this.postPredicate = postPredicate;
        this.aggregator = aggregator;
        this.sorter = sorter;
        this.equivalentPredicate = equivalentPredicate;
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                       DataTuplePredicate postPredicate, Aggregator aggregator, DataTupleSorter sorter) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, predicate, postPredicate, aggregator, sorter, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                       DataTuplePredicate postPredicate, Aggregator aggregator) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, predicate, postPredicate, aggregator, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, DataTuplePredicate predicate,
                                       DataTuplePredicate postPredicate) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, predicate, postPredicate, null, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime, Aggregator aggregator,
                                       DataTuplePredicate postPredicate) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, null, postPredicate, aggregator, null);
    }

    public NetworkTemporalQueryRequest(T destIpLowerBound, T destIpUpperBound, long startTime, long endTime) {
        this(destIpLowerBound, destIpUpperBound, startTime, endTime, null, null, null);
    }
}
