package indexingTopology.client;

import java.io.IOException;

/**
 * Created by robert on 9/3/17.
 */
public class QueryClient extends ClientSkeleton {
    public QueryClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public QueryResponse temporalRangeQuery(Number lowKey, Number highKey, long startTime, long endTime) throws IOException,
            ClassNotFoundException {
        objectOutputStream.writeUnshared(new QueryRequest<>(lowKey, highKey, startTime, endTime));
        objectOutputStream.reset();
        return (QueryResponse) objectInputStream.readUnshared();
    }

    public QueryResponse query(QueryRequest query) throws IOException  {
        objectOutputStream.writeUnshared(query);
        objectOutputStream.reset();
        try {
            return (QueryResponse) objectInputStream.readUnshared();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
