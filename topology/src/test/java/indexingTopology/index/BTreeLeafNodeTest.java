package indexingTopology.index;

import indexingTopology.common.data.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by acelzj on 11/1/16.
 */
public class BTreeLeafNodeTest {
    @Test
    public void getTuples() throws Exception, UnsupportedGenericException {
        BTreeLeafNode node = new BTreeLeafNode(4);
        for (int i = 1; i <= 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add(i*1.0);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add(j*1.0);
            }
            byte[] bytes = serializeIndexValue(values);
            node.insertKeyTuples(i*1.0, bytes, false);
        }

        assertEquals(1, node.getTuplesWithinKeyRange(2.0, 2.0).size());
        assertEquals(2, node.getTuplesWithinKeyRange(1.0, 2.0).size());
        assertEquals(3, node.getTuplesWithinKeyRange(1.0, 3.0).size());
        assertEquals(4, node.getTuplesWithinKeyRange(1.0, 4.0).size());
        assertEquals(4, node.getTuplesWithinKeyRange(0.0, 5.0).size());
        assertEquals(3, node.getTuplesWithinKeyRange(1.5, 4.0).size());
        assertEquals(1, node.getTuplesWithinKeyRange(1.5, 2.5).size());
        assertEquals(1, node.getTuplesWithinKeyRange(3.5, 5.0).size());
        assertEquals(2, node.getTuplesWithinKeyRange(2.5, 5.0).size());
        assertEquals(0, node.getTuplesWithinKeyRange(-5.0, -4.0).size());
        assertEquals(0, node.getTuplesWithinKeyRange(6.0, 10.0).size());
    }

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id","time");

    @Test
    public void testSearch() throws Exception, UnsupportedGenericException {

        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key);
            min = Math.min(min, key);
            max = Math.max(max, key);
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyTuples(key, bytes, false);
        }

        Collections.sort(keys);

        for (int i = 0; i < keys.size(); ++i) {
            Integer index = i;
            assertEquals(index, (Integer) leaf.search(keys.get(i)));
        }

        assertTrue(-1 == leaf.search(min - 1));
        assertTrue(-1 == leaf.search(max + 1));
    }

    @Test
    public void testSplit() throws Exception, UnsupportedGenericException {

        int order = 1024;

        BTreeNode root = null;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < order + 1; ++i) {
            keys.add(random.nextInt());
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            root = leaf.insertKeyTuples(key, bytes, false);
        }

        assertEquals(512, leaf.getKeyCount());
        assertEquals(512, leaf.getAtomicKeyCount());
        assertEquals(513, leaf.rightSibling.getKeyCount());
        assertEquals(513, ((BTreeLeafNode) leaf.rightSibling).getAtomicKeyCount());
        assertEquals(1, root.getKeyCount());
    }


    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {

        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(random.nextInt());
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyTuples(key, bytes, false);
        }

        for (Integer key : keys) {
            leaf.acquireReadLock();
//            List<byte[]> tuples = leaf.search(key, key);
//            assertEquals(1, tuples.size());
        }

    }


    @Test
    public void testSearchRangeAllTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < order; ++i) {
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
            leaf.insertKeyTuples(key, bytes, false);
        }

        leaf.acquireReadLock();
//        List<byte[]> tuples = leaf.search(min - 1, max + 1);
//        assertEquals(numberOfTuples,  tuples.size());

        leaf.acquireReadLock();
//        tuples = leaf.search(min, max);
//        assertEquals(numberOfTuples,  tuples.size());
    }


    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

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
            leaf.insertKeyTuples(key, bytes, false);
        }

        Collections.sort(keys);

        leaf.acquireReadLock();
//        List<byte[]> tuples = leaf.search(keys.get(300), keys.get(512));
//        assertEquals(213, tuples.size());


        leaf.acquireReadLock();
//        tuples = leaf.search(keys.get(1022), keys.get(1023));
//        assertEquals(2, tuples.size());

        leaf.acquireReadLock();
//        tuples = leaf.search(keys.get(0), keys.get(1));
//        assertEquals(2, tuples.size());
    }

    @Test
    public void testSearchRangeWithoutTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < order; ++i) {
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
            leaf.insertKeyTuples(key, bytes, false);
        }

        leaf.acquireReadLock();
//        List<byte[]> tuples = leaf.search(max + 1, Integer.MAX_VALUE);
//        assertEquals(0, tuples.size());

//        leaf.acquireReadLock();
//        tuples = leaf.search(Integer.MIN_VALUE, min - 1);
//        assertEquals(0, tuples.size());
    }



    @Test
    public void testInsertKeyValueWithoutDuplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer min = Integer.MAX_VALUE;

        Integer max = Integer.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            while (keys.contains(key)) {
                key = random.nextInt();
            }
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
            leaf.insertKeyTuples(key, bytes, false);
        }

        leaf.acquireReadLock();
//        List<byte[]> tuples = leaf.search(min, max);
//        assertEquals(numberOfTuples, tuples.size());

    }

    @Test
    public void testInsertKeyValueWithDuplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        Integer randomKey = random.nextInt();

        for (int i = 0; i < order; ++i) {
            keys.add(randomKey);
        }

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyTuples(key, bytes, false);
        }

        leaf.acquireReadLock();
//        List<byte[]> tuples = leaf.search(randomKey, randomKey);
//        assertEquals(numberOfTuples, tuples.size());
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