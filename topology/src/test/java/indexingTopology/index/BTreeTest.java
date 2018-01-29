package indexingTopology.index;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by acelzj on 21/12/16.
 */
public class BTreeTest {

    TopologyConfig config = new TopologyConfig();

    @Test
    public void testGetTemplate() throws Exception, UnsupportedGenericException {
        int order = 4;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 60;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
//            while (keys.contains(key)) {
//                key = random.nextInt();
//            }
//            keys.add(key);
            keys.add(i);
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert((double) key, bytes);
        }

        BTreeLeafNode leaf = bTree.getLeftMostLeaf();
        int numberOfLeaves = 0;
        while (leaf != null) {
            ++numberOfLeaves;
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }

//        bTree.printBtree();

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange((double) key, (double) key).size());
        }

        BTree template = bTree.getTemplate();


        bTree.getRoot().keys = null;


        for (Integer key : keys) {
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
            assertEquals(0, leaf.atomicKeyCount.get());
            assertEquals(0, leaf.tuples.size());
            assertEquals(0, leaf.offsets.size());
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

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert((double) key, bytes);
        }

        byte[] serializedLeaves = bTree.serializeLeaves();

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

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert((double) key, bytes);
        }

    }

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id","time");


    @Test
    public void testInsert() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key, bytes);
        }

        Collections.sort(keys);

        BTreeLeafNode leaf = bTree.getLeftMostLeaf();

        for (Integer key : keys) {
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

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

//        bTree.printBtree();

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key * 1.0, key * 1.0).size());
        }
        //Test template mode
       bTree.clearPayload();

//        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
//            keys.add(i + 100);
//        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key*1.0, key*1.0).size());
        }
    }

    @Test
    public void testSearchTuplesWithDumplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 64;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 65;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i);
        }

//        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
            bTree.insert(key*1.0, bytes);
        }

//        bTree.printBtree();

        for (Integer key : keys) {
            assertEquals(2, bTree.searchRange(key * 1.0, key * 1.0).size());
        }
        //Test template mode
        bTree.clearPayload();

//        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
//            keys.add(i + 100);
//        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
            bTree.insert(key*1.0, bytes);
        }

        for (Integer key : keys) {
            assertEquals(2, bTree.searchRange(key*1.0, key*1.0).size());
        }
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key*1.0, key*1.0).size());
        }

        //Test template mode
        bTree.clearPayload();

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key*1.0, key*1.0).size());
        }

    }


    @Test
    public void testSearchRangeLeftKeyAndRightAllTuples() throws Exception, UnsupportedGenericException {
        int order = 4;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 32;

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

//        bTree.printBtree();

        assertEquals(numberOfTuples, bTree.searchRange(0.0, numberOfTuples*1.0).size());

        //Test template mode
        bTree.clearPayload();

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

        assertEquals(numberOfTuples, bTree.searchRange(0.0, numberOfTuples*1.0).size());

    }

    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key*1.0, bytes);
        }

        Collections.sort(keys);

//        bTree.printBtree();

        List<byte[]> tuples = bTree.searchRange(keys.get(300)*1.0, keys.get(512)*1.0);
//        for (int i = 0; i < tuples.size(); ++i) {
//            System.out.println(deserialize(tuples.get(i)));
//        }
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

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key);
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key, bytes);
        }

        bTree.clearPayload();
        BTreeLeafNode leaf = bTree.getLeftMostLeaf();
        while (leaf != null) {
            assertEquals(0, leaf.keyCount);
            assertEquals(0, leaf.tuples.size());
            assertEquals(0, leaf.offsets.size());
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }
    }

    @Test
    public void clearPayloadInTemplateMode() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, config);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key);
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key, bytes);
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
            assertEquals(0, leaf.tuples.size());
            assertEquals(0, leaf.offsets.size());
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