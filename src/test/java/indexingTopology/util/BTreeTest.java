package indexingTopology.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 21/12/16.
 */
public class BTreeTest {
    @Test
    public void testClone() throws Exception, UnsupportedGenericException {
        int order = 4;
        BTree bTree = new BTree(order);

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


        BTree newBTree = bTree.clone();

        System.out.println(newBTree == bTree);

        bTree.getRoot().keys = null;

        newBTree.printBtree();

        for (Integer key : keys) {
            assertEquals(1, newBTree.searchRange((double) key, (double) key).size());
        }

        BTreeLeafNode leaf = newBTree.getLeftMostLeaf();

        while (leaf != null) {
            leaf.print();
            leaf.parentNode.print();
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }

        newBTree.clearPayload();

        leaf = newBTree.getLeftMostLeaf();
        while (leaf != null) {
            assertEquals(0, leaf.bytesCount);
            assertEquals(0, leaf.keyCount);
            assertEquals(0, leaf.tuples.size());
            assertEquals(0, leaf.offsets.size());
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }
    }

    @Test
    public void serializeLeaves() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

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
            bTree.insert((double) key, bytes);
        }

        byte[] serializedLeaves = bTree.serializeLeaves();

        Input input = new Input(serializedLeaves, 4, serializedLeaves.length);

        Kryo kryo = new Kryo();
        kryo.register(BTreeLeafNode.class, new KryoLeafNodeSerializer());

        while (input.position() < serializedLeaves.length) {
            BTreeLeafNode leaf = kryo.readObject(input, BTreeLeafNode.class);
//            leaf.print();
            input.setPosition(input.position() + 4);
        }
    }

    @Test
    public void writeLeavesIntoChunk() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

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
            bTree.insert((double) key, bytes);
        }

    }

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");


    @Test
    public void testInsert() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

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
        BTree bTree = new BTree(order);

        int numberOfTuples = 64;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
            keys.add(i);
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


        bTree.printBtree();

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
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
            bTree.insert(key, bytes);
        }

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
        }
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

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

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
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
            bTree.insert(key, bytes);
        }

        for (Integer key : keys) {
            assertEquals(1, bTree.searchRange(key, key).size());
        }

    }


    @Test
    public void testSearchRangeLeftKeyAndRightAllTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            min = Math.min(min, key);
            max = Math.max(max, key);
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

        assertEquals(numberOfTuples, bTree.searchRange(min, max).size());

        //Test template mode
        bTree.clearPayload();

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(key, bytes);
        }

        assertEquals(numberOfTuples, bTree.searchRange(min, max).size());

    }

    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

        int numberOfTuples = 2048;

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            min = Math.min(min, key);
            max = Math.max(max, key);
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

        Collections.sort(keys);

        List<byte[]> tuples = bTree.searchRange(keys.get(300), keys.get(512));
        assertEquals(213, tuples.size());

        tuples = bTree.searchRange(keys.get(1022), keys.get(1023));
        assertEquals(2, tuples.size());

        tuples = bTree.searchRange(keys.get(0), keys.get(1));
        assertEquals(2, tuples.size());

    }

    @Test
    public void clearPayload() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

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
            assertEquals(0, leaf.bytesCount);
            assertEquals(0, leaf.keyCount);
            assertEquals(0, leaf.tuples.size());
            assertEquals(0, leaf.offsets.size());
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }
    }

    @Test
    public void clearPayloadInTemplateMode() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order);

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

        TemplateUpdater templateUpdater = new TemplateUpdater(32);
        bTree = templateUpdater.createTreeWithBulkLoading(bTree);


        bTree.clearPayload();
        BTreeLeafNode leaf = bTree.getLeftMostLeaf();
        while (leaf != null) {
            assertEquals(0, leaf.bytesCount);
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

}