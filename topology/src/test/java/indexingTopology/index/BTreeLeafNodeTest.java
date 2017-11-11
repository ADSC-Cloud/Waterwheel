package indexingTopology.index;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.index.BTreeLeafNode;
import indexingTopology.index.BTreeNode;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 11/1/16.
 */
public class BTreeLeafNodeTest {
    public void setUp() {
        DataSchema schema = new DataSchema();
        schema.addDoubleField("a1");
        schema.setPrimaryIndexField("a1");
    }


    @Test
    public void getTuples() throws Exception, UnsupportedGenericException {
        BTreeLeafNode node = new BTreeLeafNode(4);
        for (int i = 1; i <= 4; ++i) {
            DataTuple dataTuple = new DataTuple((double) i);
            node.insertKeyTuples((double) i, dataTuple, false);
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

    @Test
    public void testSearch() throws Exception, UnsupportedGenericException {

        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        Double min = Double.MAX_VALUE;

        Double max = Double.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key * 1.0);
            min = Math.min(min, key * 1.0);
            max = Math.max(max, key);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
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

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < order + 1; ++i) {
            keys.add(random.nextInt() * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            root = leaf.insertKeyTuples(key, dataTuple, false);
        }

        assertEquals(512, leaf.getKeyCount());
        assertEquals(512, leaf.getAtomicTupleCount());
        assertEquals(513, leaf.rightSibling.getKeyCount());
        assertEquals(513, ((BTreeLeafNode) leaf.rightSibling).getAtomicTupleCount());
        assertEquals(1, root.getKeyCount());
    }


    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {

        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            keys.add(random.nextInt() * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
        }

        for (Double key : keys) {
            leaf.acquireReadLock();
            List<DataTuple> tuples = leaf.getTuplesWithinKeyRange(key, key);
            assertEquals(1, tuples.size());
        }

    }


    @Test
    public void testSearchRangeAllTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        Double min = Double.MAX_VALUE;

        Double max = Double.MIN_VALUE;

        for (int i = 0; i < order; ++i) {
            Integer key = random.nextInt();
            min = Math.min(min, key);
            max = Math.max(max, key);
            keys.add(key * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
        }

        leaf.acquireReadLock();
        List<DataTuple> tuples = leaf.getTuplesWithinKeyRange(min - 1, max + 1);
        assertEquals(numberOfTuples,  tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.getTuplesWithinKeyRange(min, max);
        assertEquals(numberOfTuples,  tuples.size());
    }


    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            keys.add(key * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
        }

        Collections.sort(keys);

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.getTuplesWithinKeyRange(keys.get(300), keys.get(512));
        assertEquals(213, tuples.size());


        leaf.acquireReadLock();
        tuples = leaf.getTuplesWithinKeyRange(keys.get(1022), keys.get(1023));
        assertEquals(2, tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.getTuplesWithinKeyRange(keys.get(0), keys.get(1));
        assertEquals(2, tuples.size());
    }

    @Test
    public void testSearchRangeWithoutTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        Double min = Double.MAX_VALUE;

        Double max = Double.MIN_VALUE;

        for (int i = 0; i < order; ++i) {
            Integer key = random.nextInt();
            min = Math.min(min, key);
            max = Math.max(max, key);
            keys.add(key * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
        }

        leaf.acquireReadLock();
        List<DataTuple> tuples = leaf.getTuplesWithinKeyRange((max + 1) * 1.0, Integer.MAX_VALUE * 1.0);
        assertEquals(0, tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.getTuplesWithinKeyRange(Integer.MIN_VALUE * 1.0, (min - 1) * 1.0);
        assertEquals(0, tuples.size());
    }



    @Test
    public void testInsertKeyValueWithoutDuplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        Double min = Double.MAX_VALUE;

        Double max = Double.MIN_VALUE;

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            while (keys.contains(key)) {
                key = random.nextInt();
            }
            min = Math.min(min, key);
            max = Math.max(max, key);
            keys.add(key * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
        }

        leaf.acquireReadLock();
        List<DataTuple> tuples = leaf.getTuplesWithinKeyRange(min, max);
        assertEquals(numberOfTuples, tuples.size());

    }

    @Test
    public void testInsertKeyValueWithDuplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order);

        Random random = new Random();

        List<Double> keys = new ArrayList<>();

        Integer randomKey = random.nextInt();

        for (int i = 0; i < order; ++i) {
            keys.add(randomKey * 1.0);
        }

        for (Double key : keys) {
            DataTuple dataTuple = new DataTuple(key);
            leaf.insertKeyTuples(key, dataTuple, false);
        }

        leaf.acquireReadLock();
        List<DataTuple> tuples = leaf.getTuplesWithinKeyRange(randomKey * 1.0, randomKey * 1.0);
        assertEquals(numberOfTuples, tuples.size());
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