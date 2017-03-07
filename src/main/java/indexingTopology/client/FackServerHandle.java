package indexingTopology.client;

import indexingTopology.DataTuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by robert on 3/3/17.
 */
public class FackServerHandle extends ServerHandle {

    public FackServerHandle(Socket client) {
        super(client);
    }

    @Override
    Response handleClientQueryRequest(ClientQueryRequest clientQueryRequest) {
        DataTuple dataTuple = new DataTuple();
        dataTuple.add("ID 1");
        dataTuple.add(100);
        dataTuple.add(3.14);

        DataTuple dataTuple1 = new DataTuple();
        dataTuple1.add("ID 2");
        dataTuple1.add(200);
        dataTuple1.add(6.34);

        QueryResult queryResult = new QueryResult();
        queryResult.add(dataTuple);
        queryResult.add(dataTuple1);
        return queryResult;
    }

    @Override
    Response handleTupleAppend(DataTuple tuple) {
        return null;
    }

}
