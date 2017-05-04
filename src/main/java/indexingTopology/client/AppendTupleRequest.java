package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by robert on 8/3/17.
 */
public class AppendTupleRequest extends ClientRequest {
    public DataTuple dataTuple;
    AppendTupleRequest(DataTuple dataTuple) {
        this.dataTuple = dataTuple;
    }
}
