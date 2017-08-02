package indexingTopology.api.server;

import indexingTopology.api.client.SchemaQueryRequest;

import java.io.IOException;

/**
 * Created by Robert on 8/2/17.
 */
public interface SchemaQueryHandle {
    void handle(SchemaQueryRequest request) throws IOException;
}
