package indexingTopology.api.server;

import indexingTopology.api.client.NetworkTemporalQueryRequest;

import java.io.IOException;

/**
 * Created by acelzj on 6/19/17.
 */
public interface NetworkTemporalQueryHandle {
    void handle(NetworkTemporalQueryRequest clientQueryRequest) throws IOException;
}
