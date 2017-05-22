package indexingTopology.client;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.SocketTimeoutException;

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
        try {
            boolean eof = false;
            QueryResponse response = null;
            while (!eof) {
                try {
                    QueryResponse remainingQueryResponse = (QueryResponse) objectInputStream.readUnshared();
                    if (response == null) {
                        response = remainingQueryResponse;
                    } else {
                        response.dataTuples.addAll(remainingQueryResponse.dataTuples);
                    }
                    eof = remainingQueryResponse.getEOFFlag();
                } catch (SocketTimeoutException e) {
                }
            }
            return response;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public QueryResponse query(QueryRequest query) throws IOException  {
        objectOutputStream.writeUnshared(query);
        objectOutputStream.reset();

        try {
            boolean eof = false;
            QueryResponse response = null;
            while (!eof) {
                try {
                    QueryResponse remainingQueryResponse = (QueryResponse) objectInputStream.readUnshared();
                    if (response == null) {
                        response = remainingQueryResponse;
                    } else {
                        response.dataTuples.addAll(remainingQueryResponse.dataTuples);
                    }
                    eof = remainingQueryResponse.getEOFFlag();
                } catch (SocketTimeoutException e) {
                }
            }
            return response;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
