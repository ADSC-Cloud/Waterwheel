package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;

/**
 * Created by robert on 9/3/17.
 */
public class OneTuplePerTransferIngestionClient extends Client implements IngestionClient {
    public OneTuplePerTransferIngestionClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public Response append(DataTuple dataTuple) throws IOException, ClassNotFoundException {
        objectOutputStream.writeObject(new AppendTupleRequest(dataTuple));
//        return (Response) objectInputStream.readObject();
        return null;
    }

    @Override
    public void appendInBatch(DataTuple tuple) throws IOException, ClassNotFoundException {

    }
}
