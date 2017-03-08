package indexingTopology.client;

import indexingTopology.data.DataTuple;
import indexingTopology.data.PartialQueryResult;

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

        PartialQueryResult particalQueryResult = new PartialQueryResult();
        particalQueryResult.add(dataTuple);
        particalQueryResult.add(dataTuple1);
        return particalQueryResult;
    }

    @Override
    Response handleTupleAppend(DataTuple tuple) {
        return null;
    }

}
