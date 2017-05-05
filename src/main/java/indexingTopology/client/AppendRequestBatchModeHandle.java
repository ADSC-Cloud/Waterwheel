package indexingTopology.client;

import indexingTopology.data.DataTupleBlock;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface AppendRequestBatchModeHandle {
    void handle(DataTupleBlock tuple) throws IOException;
}
