package indexingTopology.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by robert on 3/5/17.
 */
public class DataTupleBlock implements Serializable {

    DataSchema scheme;
    int capacity;
    public transient List<DataTuple> tuples;
    private List<byte[]> byteArrays;

    public DataTupleBlock(DataSchema scheme, int capacity) {
        this.scheme = scheme;
        this.capacity = capacity;
        this.tuples = new ArrayList<>();
    }

    public boolean add(DataTuple dataTuple) {
        if (tuples.size() >= capacity)
            return false;
        tuples.add(dataTuple);
        return true;
    }

    public void serialize() {
        byteArrays = new ArrayList<>();
        tuples.forEach(t -> byteArrays.add(scheme.serializeTuple(t)));
    }

    public void deserialize() {
        tuples = new ArrayList<>();
        byteArrays.forEach(e -> tuples.add(scheme.deserializeToDataTuple(e)));
    }
}
