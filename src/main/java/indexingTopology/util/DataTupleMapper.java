package indexingTopology.util;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by robert on 15/3/17.
 */
public class DataTupleMapper implements Serializable {

    Function<DataTuple,DataTuple> mapFunction;
    DataSchema inputSchema;

    public DataTupleMapper(DataSchema inputSchema, Function<DataTuple, DataTuple> mapFunction) {
        this.mapFunction = mapFunction;
        this.inputSchema = inputSchema;
    }

    public DataTuple map(DataTuple dataTuple) {
       return mapFunction.apply(dataTuple);
    }

    public DataSchema getOriginalSchema() {
        return inputSchema;
    }

    public static void main(String[] args) {

    }
}
