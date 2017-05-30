package indexingTopology.client;

import indexingTopology.data.DataTupleBlock;

/**
 * Created by robert on 8/3/17.
 */
public class AppendRequestBatchMode extends IClientRequest {
    public DataTupleBlock dataTupleBlock;
    public boolean requireAck;
    AppendRequestBatchMode(DataTupleBlock dataTupleBlock) {
        this.dataTupleBlock = dataTupleBlock;
    }
    AppendRequestBatchMode(DataTupleBlock dataTupleBlock, boolean requireAck) {
        this.dataTupleBlock = dataTupleBlock;
        this.requireAck = requireAck;
    }
}
