package indexingTopology.client;

import indexingTopology.data.DataTupleBlock;

import java.io.IOException;

/**
 * Created by robert on 8/3/17.
 */
public interface IAppendRequestBatchModeHandle {
    void handle(AppendRequestBatchMode tuple) throws IOException;
}
