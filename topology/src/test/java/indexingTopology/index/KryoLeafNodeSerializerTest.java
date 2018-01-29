package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by acelzj on 1/4/17.
 */
public class KryoLeafNodeSerializerTest {

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));

    private TopologyConfig config = new TopologyConfig();

    @Test
    public void testSerializer() throws IOException, UnsupportedGenericException {

        Kryo kryo = new Kryo();
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

        BTreeLeafNode leaf = new BTreeLeafNode(config.BTREE_ORDER);

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();

            for (int j = 0; j < fieldNames.size(); ++j) {
                values.add((double) j);
            }


            byte[] bytes = serializeIndexValue(values);

            leaf.insertKeyTuples((double) i, bytes ,false);
        }


//        int totalBytes = leaf.getBytesCount() + (1 + leaf.getKeys().size()) * (Integer.SIZE / Byte.SIZE);

//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        Output output = new Output(200000, 2000000);

        kryo.writeObject(output, leaf);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);

        leaf = kryo.readObject(input, BTreeLeafNode.class);

        for (int i = 0; i < 1024; ++i) {
            leaf.acquireReadLock();
            List<byte[]> serializedTuples = leaf.getTuplesWithinKeyRange((double) i, (double) i);
//            leaf.releaseReadLock();
//            for (int j = 0; j < serializedTuples.size(); ++j) {
//                System.out.println(deserialize(serializedTuples.get(j)));
//            }
        }
        output.close();
    }

    @Test
    public void testSerializerWithDumplicateKeys() throws IOException, UnsupportedGenericException {

        Kryo kryo = new Kryo();
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

        BTreeLeafNode leaf = new BTreeLeafNode(config.BTREE_ORDER);

        for (int i = 0; i < 64; ++i) {
            List<Double> values = new ArrayList<>();

            for (int j = 0; j < fieldNames.size(); ++j) {
                values.add((double) j);
            }


            byte[] bytes = serializeIndexValue(values);

            leaf.insertKeyTuples((double) i, bytes ,false);
            leaf.insertKeyTuples((double) i, bytes ,false);
            leaf.insertKeyTuples((double) i, bytes ,false);
        }


//        int totalBytes = leaf.getBytesCount() + (1 + leaf.getKeys().size()) * (Integer.SIZE / Byte.SIZE);

//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        Output output = new Output(200000, 2000000);

        kryo.writeObject(output, leaf);

//        output.close();

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);

        leaf = kryo.readObject(input, BTreeLeafNode.class);

        for (int i = 0; i < 64; ++i) {
            leaf.acquireReadLock();
            List<byte[]> serializedTuples = leaf.getTuplesWithinKeyRange((double) i, (double) i);

            assertEquals(3, serializedTuples.size());

            for (int j = 0; j < serializedTuples.size(); ++j) {
                deserialize(serializedTuples.get(j));
            }
//            leaf.releaseReadLock();
//            for (int j = 0; j < serializedTuples.size(); ++j) {
//                System.out.println(deserialize(serializedTuples.get(j)));
//            }
        }
        output.close();
    }

    public byte[] serializeIndexValue(List<Double> values) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        for (int i = 0;i < valueTypes.size(); ++i) {
            if (valueTypes.get(i).equals(Double.class)) {
                byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(i)).array();
                bos.write(b);
            }
        }

        byte [] b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putDouble(System.currentTimeMillis()).array();
        bos.write(b);
        return bos.toByteArray();
    }


    public Values deserialize(byte [] b) throws IOException {
        Values values = new Values();
        int offset = 0;
        for (int i = 0; i < valueTypes.size(); i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                int len = Double.SIZE/Byte.SIZE;
                double val = ByteBuffer.wrap(b, offset, len).getDouble();
                values.add(val);
                offset += len;
            }
        }

        int len = Double.SIZE / Byte.SIZE;
        Double val = ByteBuffer.wrap(b, offset, len).getDouble();
        values.add(val);
        return values;
    }

}