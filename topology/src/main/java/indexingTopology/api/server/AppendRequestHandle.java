package indexingTopology.api.server;

import indexingTopology.api.client.AppendRequest;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface AppendRequestHandle {
    void handle(AppendRequest tuple) throws IOException;
}
