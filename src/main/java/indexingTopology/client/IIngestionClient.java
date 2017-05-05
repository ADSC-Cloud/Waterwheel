package indexingTopology.client;

import indexingTopology.data.DataTuple;

import java.io.IOException;

/**
 * Created by robert on 3/5/17.
 */
interface IIngestionClient {
    IResponse append(DataTuple tuple) throws IOException, ClassNotFoundException ;
    void appendInBatch(DataTuple tuple) throws IOException, ClassNotFoundException;
    void flush() throws IOException, ClassNotFoundException;
}
