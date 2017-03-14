package indexingTopology.client;

import indexingTopology.util.Query;

import java.io.IOException;

/**
 * Created by robert on 9/3/17.
 */
public class QueryClient extends Client {
    public QueryClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public QueryResponse temporalRangeQuery(Number lowKey, Number highKey, long startTime, long endTime) throws IOException,
            ClassNotFoundException {
        objectOutputStream.writeObject(new QueryRequest<Number>(lowKey, highKey, startTime, endTime));
        return (QueryResponse) objectInputStream.readObject();
    }

    public QueryResponse query(QueryRequest query) throws IOException, ClassNotFoundException  {
        objectOutputStream.writeObject(query);
        return (QueryResponse) objectInputStream.readObject();
    }
}
