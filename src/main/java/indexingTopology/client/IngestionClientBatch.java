package indexingTopology.client;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.DataTupleBlock;

import java.io.IOException;

/**
 * Created by robert on 9/3/17.
 */
public class IngestionClientBatch extends Client implements IngestionClient {

    DataSchema schema;
    int batchSize;

    DataTupleBlock dataTupleBlock;

    public IngestionClientBatch(String serverHost, int port, DataSchema schema, int batchSize) {
        super(serverHost, port);
        this.schema = schema;
        this.batchSize = batchSize;
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
    }

    public Response append(DataTuple dataTuple) throws IOException, ClassNotFoundException {

        objectOutputStream.writeObject(dataTuple);

//        return (Response) objectInputStream.readObject();
        return null;
    }

    @Override
    public void appendInBatch(DataTuple tuple) throws IOException, ClassNotFoundException {
        while (!dataTupleBlock.add(tuple)) {
            dataTupleBlock.serialize();
            objectOutputStream.writeObject(dataTupleBlock);
            dataTupleBlock = new DataTupleBlock(schema, batchSize);
        }
    }
}
