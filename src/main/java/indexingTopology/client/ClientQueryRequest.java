package indexingTopology.client;

/**
 * Created by robert on 3/3/17.
 */
public class ClientQueryRequest<T extends Number>  extends ClientRequest{
    T low;
    T high;
    long startTime;
    long endTime;
    public ClientQueryRequest(T low, T high, long startTime, long endTime) {
        this.low = low;
        this.high = high;
        this.startTime = startTime;
        this.endTime = endTime;
    }
}
