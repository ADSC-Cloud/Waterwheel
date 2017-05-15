package indexingTopology.client;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.DataTupleBlock;

import java.io.IOException;
import java.net.SocketException;

/**
 * Created by robert on 9/3/17.
 */
public class IngestionClientBatchMode extends ClientSkeleton implements IIngestionClient {

    DataSchema schema;
    int batchSize;

    DataTupleBlock dataTupleBlock;

    public IngestionClientBatchMode(String serverHost, int port, DataSchema schema, int batchSize) {
        super(serverHost, port);
        this.schema = schema;
        this.batchSize = batchSize;
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
    }

    public IResponse append(DataTuple dataTuple) throws IOException {

        appendInBatch(dataTuple);

//        return (Response) objectInputStream.readObject();
        return null;
    }

    @Override
    public void appendInBatch(DataTuple tuple) throws IOException {
        while (!dataTupleBlock.add(tuple)) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        dataTupleBlock.serialize();
        objectOutputStream.writeUnshared(new AppendRequestBatchMode(dataTupleBlock));
        objectOutputStream.reset();
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
//        objectInputStream.readUnshared();
    }
}
