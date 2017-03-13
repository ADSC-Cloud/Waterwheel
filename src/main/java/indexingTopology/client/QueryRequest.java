package indexingTopology.client;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.util.DataTuplePredicate;

import java.util.function.Predicate;

/**
 * Created by robert on 3/3/17.
 */
public class QueryRequest<T extends Number>  extends ClientRequest{
    final public T low;
    final public T high;
    final public long startTime;
    final public long endTime;
    final public DataTuplePredicate predicate;
    final public Aggregator aggregator;
    public QueryRequest(T low, T high, long startTime, long endTime, DataTuplePredicate predicate, Aggregator aggregator) {
        this.low = low;
        this.high = high;
        this.startTime = startTime;
        this.endTime = endTime;
        this.predicate = predicate;
        this.aggregator = aggregator;
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
