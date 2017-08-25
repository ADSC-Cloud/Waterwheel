package indexingTopology.common.data;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
    private byte[] bytes;

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
        Output output = new Output(8000, 2000000);

        tuples.forEach(t -> {
            byte[] bytes = scheme.serializeTuple(t);
            output.writeShort((short)bytes.length);
            output.writeBytes(bytes);
        });

        bytes = output.toBytes();
        output.close();

    }

    public void deserialize() {
        tuples = new ArrayList<>();

        Input input = new Input(bytes);
        while(!input.eof()) {
            int length = input.readShort();
            byte[] serializedTuple = input.readBytes(length);
            tuples.add(scheme.deserializeToDataTuple(serializedTuple));
        }
    }
}
