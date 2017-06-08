package indexingTopology.api.client;

import indexingTopology.common.data.DataTuple;

/**
 * Created by robert on 8/3/17.
 */
public class AppendRequest extends IClientRequest {
    public DataTuple dataTuple;
    AppendRequest(DataTuple dataTuple) {
        this.dataTuple = dataTuple;
    }
}
