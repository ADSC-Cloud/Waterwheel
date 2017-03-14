package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;

/**
 * Created by robert on 9/3/17.
 */
public class IngestionClient extends Client {
    public IngestionClient(String serverHost, int port) {
        super(serverHost, port);
    }

    public Response append(DataTuple dataTuple) throws IOException, ClassNotFoundException {
        objectOutputStream.writeObject(new AppendRequest(dataTuple));
        return (Response) objectInputStream.readObject();
    }
}
