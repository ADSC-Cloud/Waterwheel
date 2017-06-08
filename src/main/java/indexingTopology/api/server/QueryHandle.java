package indexingTopology.api.server;

import indexingTopology.api.client.QueryRequest;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface QueryHandle {
    void handle(QueryRequest clientQueryRequest) throws IOException;
}
