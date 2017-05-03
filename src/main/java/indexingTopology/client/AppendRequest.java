package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by robert on 8/3/17.
 */
public class AppendRequest extends ClientRequest {
    public DataTuple dataTuple;
    AppendRequest(DataTuple dataTuple) {
        this.dataTuple = dataTuple;
    }
}
