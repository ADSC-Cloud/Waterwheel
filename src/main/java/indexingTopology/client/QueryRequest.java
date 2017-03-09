package indexingTopology.client;

/**
 * Created by robert on 3/3/17.
 */
public class QueryRequest<T extends Number>  extends ClientRequest{
    final public T low;
    final public T high;
    final public long startTime;
    final public long endTime;
    public QueryRequest(T low, T high, long startTime, long endTime) {
        this.low = low;
        this.high = high;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
