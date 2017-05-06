package indexingTopology.client;

import java.io.IOException;

/**
 * Created by robert on 9/3/17.
 */
public class GeoTemporalQueryClient extends QueryClient {
    public GeoTemporalQueryClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public QueryResponse query(GeoTemporalQueryRequest queryRequest) throws IOException, ClassNotFoundException {
        objectOutputStream.writeUnshared(queryRequest);
        objectOutputStream.reset();
        return (QueryResponse) objectInputStream.readUnshared();
    }

    public QueryResponse temporalRangeQuery(Number lowKey, Number highKey, long startTime, long endTime) throws IOException,
            ClassNotFoundException {
        objectOutputStream.writeUnshared(new QueryRequest<>(lowKey, highKey, startTime, endTime));
        objectOutputStream.reset();
        return (QueryResponse) objectInputStream.readUnshared();
    }

    public QueryResponse query(QueryRequest query) throws IOException, ClassNotFoundException  {
        objectOutputStream.writeUnshared(query);
        objectOutputStream.reset();
        return (QueryResponse) objectInputStream.readUnshared();
    }
}
