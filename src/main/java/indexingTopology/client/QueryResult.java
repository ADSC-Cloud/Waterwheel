package indexingTopology.client;


import indexingTopology.DataTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by robert on 3/3/17.
 */
public class QueryResult extends Response {

    List<DataTuple> dataTuples = new ArrayList<>();

    public String toString() {
        String ret = "";
        for(DataTuple tuple: dataTuples) {
            ret += tuple + "\n";
        }
        return ret;
    }

    public void add(DataTuple tuple) {
        dataTuples.add(tuple);
    }
}
