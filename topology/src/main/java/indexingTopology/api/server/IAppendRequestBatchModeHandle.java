package indexingTopology.api.server;

import indexingTopology.api.client.AppendRequestBatchMode;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface IAppendRequestBatchModeHandle {
    void handle(AppendRequestBatchMode tuple) throws IOException;
}
