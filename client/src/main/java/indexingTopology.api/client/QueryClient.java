package indexingTopology.api.client;

import indexingTopology.common.data.DataSchema;

import java.io.IOException;
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
//        objectOutputStream.flush();
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
                    if (isClosed()) {
                        throw e;
                    }
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
                        System.out.println("remainingQueryResponse.dataTuples:" + remainingQueryResponse.dataTuples.size());
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

    public DataSchema querySchema() throws IOException {
        return querySchema("default");
    }

    public DataSchema querySchema(String name) throws IOException {
        SchemaQueryRequest schemaQueryRequest = new SchemaQueryRequest(name);
        objectOutputStream.writeUnshared(schemaQueryRequest);
        objectOutputStream.reset();
        try {
            DataSchema schema = (DataSchema) objectInputStream.readUnshared();
            return schema;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean createSchema(String name, DataSchema schema) throws IOException{
        if (schema == null) {
            return false;
        }
        SchemaCreationRequest schemaCreationRequest = new SchemaCreationRequest(name, schema);
        objectOutputStream.writeUnshared(schemaCreationRequest);
        objectOutputStream.reset();
        try {
            SchemaCreationResponse response = (SchemaCreationResponse) objectInputStream.readUnshared();
            return response.status == 0;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }
}
