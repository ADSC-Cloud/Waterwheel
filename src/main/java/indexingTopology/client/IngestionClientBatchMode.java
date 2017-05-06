package indexingTopology.client;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.DataTupleBlock;

import java.io.IOException;

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

    public IResponse append(DataTuple dataTuple) throws IOException, ClassNotFoundException {

        objectOutputStream.writeUnshared(dataTuple);

//        return (Response) objectInputStream.readObject();
        return null;
    }

    @Override
    public void appendInBatch(DataTuple tuple) throws IOException, ClassNotFoundException {
        while (!dataTupleBlock.add(tuple)) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException, ClassNotFoundException {
        dataTupleBlock.serialize();
        objectOutputStream.writeUnshared(new AppendRequestBatchMode(dataTupleBlock));
        objectOutputStream.reset();
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
//        objectInputStream.readUnshared();
    }
}
