package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.index.BTree;
import indexingTopology.index.BTreeLeafNode;
import indexingTopology.index.KryoLeafNodeSerializer;
import indexingTopology.index.TemplateUpdater;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 21/12/16.
 */
public class BTreeTest {
    DataSchema schema = new DataSchema();
    public void setUp() {
        schema.addDoubleField("a1");
        schema.setPrimaryIndexField("a1");
    }

    TopologyConfig config = new TopologyConfig();


    @Test
    public void testGetTemplate() throws Exception, UnsupportedGenericException {
        int order = 4;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 60;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        BTreeLeafNode leaf = bTree.getLeftMostLeaf();
        int numberOfLeaves = 0;
        while (leaf != null) {
            ++numberOfLeaves;
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }

//        bTree.printBtree();

        for (Double key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
        }

        BTree template = bTree.getTemplate();


        bTree.getRoot().keys = null;


        for (Double key : keys) {
            assertEquals(0, template.searchRange((double) key, (double) key).size());
        }


        leaf = template.getLeftMostLeaf();

        while (leaf != null) {
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }

//        newBTree.clearPayload();

        int count = 0;
        leaf = template.getLeftMostLeaf();
        while (leaf != null) {
            assertEquals(0, leaf.keyCount);
            assertEquals(0, leaf.dataTuples.size());
            assertEquals(0, leaf.atomicTupleCount.get());

            leaf = (BTreeLeafNode) leaf.rightSibling;
            ++count;
        }

        assertEquals(count, numberOfLeaves);
    }

    @Test
    public void serializeLeaves() throws Exception, UnsupportedGenericException {

        BTree bTree = new BTree(config.BTREE_ORDER, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        byte[] serializedLeaves = bTree.serializeLeaves(schema);

        Input input;
        if (config.ChunkOrientedCaching) {
            input = new Input(serializedLeaves, 0, serializedLeaves.length);
        } else {
            input = new Input(serializedLeaves, 4, serializedLeaves.length);
        }

        Kryo kryo = new Kryo();
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer(config));

        while (input.position() < serializedLeaves.length) {
            BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);
//            leaf.print();
            if (!config.ChunkOrientedCaching) {
                input.setPosition(input.position() + 4);
            }
        }
    }

    @Test
    public void writeLeavesIntoChunk() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

    }

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));


    @Test
    public void testInsert() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        Collections.sort(keys);

        BTreeLeafNode leaf = bTree.getLeftMostLeaf();

        for (Double key : keys) {
            if (leaf != null) {
                if (leaf.search(key) < 0) {
                    leaf = (BTreeLeafNode) leaf.rightSibling;
                }
                assertTrue(leaf.search(key) >= 0);
            }
        }
    }

    @Test
    public void testSearchTuples() throws Exception, UnsupportedGenericException {
        int order = 4;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 64;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

//        bTree.printBtree();

        for (Double key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
        }
        //Test template mode
       bTree.clearPayload();

//        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
//            keys.add(i + 100);
//        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        for (Double key : keys) {
            assertEquals(1, bTree.searchRange(key*1.0, key*1.0).size());
        }
    }

    @Test
    public void testSearchTuplesWithDumplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 64;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 65;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i * 1.0);
        }

//        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key*1.0, dataTuple);
            bTree.insert(key*1.0, dataTuple);
        }

//        bTree.printBtree();

        for (Double key : keys) {
            assertEquals(2, bTree.searchRange(key , key).size());
        }
        //Test template mode
        bTree.clearPayload();

//        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
//            keys.add(i + 100);
//        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key*1.0, dataTuple);
            bTree.insert(key*1.0, dataTuple);
        }

        for (Double key : keys) {
            assertEquals(2, bTree.searchRange(key*1.0, key*1.0).size());
        }
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {

            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key*1.0, dataTuple);
        }

        for (Double key : keys) {
            assertEquals(1, bTree.searchRange(key*1.0, key*1.0).size());
        }

        //Test template mode
        bTree.clearPayload();

        for (Double key : keys) {

            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key*1.0, dataTuple);
        }

        for (Double key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
        }

    }


    @Test
    public void testSearchRangeLeftKeyAndRightAllTuples() throws Exception, UnsupportedGenericException {
        int order = 4;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 32;

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

//        bTree.printBtree();

        assertEquals(numberOfTuples, bTree.searchRange(0.0, numberOfTuples*1.0).size());

        //Test template mode
        bTree.clearPayload();

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        assertEquals(numberOfTuples, bTree.searchRange(0.0, numberOfTuples*1.0).size());

    }

    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        Double min = Double.MAX_VALUE;

        Double max = Double.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i * 1.0);
        }

        Collections.shuffle(keys);

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        Collections.sort(keys);

//        bTree.printBtree();

        List<byte[]> tuples = bTree.searchRange(keys.get(300)*1.0, keys.get(512)*1.0);
        assertEquals(213, tuples.size());

        tuples = bTree.searchRange(keys.get(1022)*1.0, keys.get(1023)*1.0);
        assertEquals(2, tuples.size());

        tuples = bTree.searchRange(keys.get(0)*1.0, keys.get(1)*1.0);
        assertEquals(2, tuples.size());

    }

    @Test
    public void clearPayload() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

        bTree.clearPayload();
        BTreeLeafNode leaf = bTree.getLeftMostLeaf();
        while (leaf != null) {
            assertEquals(0, leaf.keyCount);
            assertEquals(0, leaf.dataTuples.size());
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }
    }

    @Test
    public void clearPayloadInTemplateMode() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            bTree.insert(key, dataTuple);
        }

//        bTree.clearPayload();

//        for (Integer key : keys) {
//            List<Double> values = new ArrayList<>();
//            values.add((double) key);
//            for (int j = 0; j < fieldNames.size() + 1; ++j) {
//                values.add((double) j);
//            }
//            byte[] bytes = serializeIndexValue(values);
//            bTree.insert(key, bytes);
//        }

        TemplateUpdater templateUpdater = new TemplateUpdater(32, config);
        bTree = templateUpdater.createTreeWithBulkLoading(bTree);


        bTree.clearPayload();
        BTreeLeafNode leaf = bTree.getLeftMostLeaf();
        while (leaf != null) {
            assertEquals(0, leaf.keyCount);
            assertEquals(0, leaf.dataTuples.size());
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }

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
        byte [] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble((Double) values.get(valueTypes.size() + 1)).array();
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