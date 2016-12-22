package indexingTopology.util;

import org.apache.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 11/1/16.
 */
public class BTreeLeafNodeTest {

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

    @Test
    public void testSplit() throws Exception, UnsupportedGenericException {

        int order = 1024;

        BTreeNode root = null;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            root = leaf.insertKeyValue(key, bytes);
        }

        assertEquals(512, leaf.getKeyCount());
        assertEquals(513, leaf.rightSibling.getKeyCount());
        assertEquals(1, root.getKeyCount());
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {

        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            leaf.insertKeyValue(key, bytes);
        }

        for (Integer key : keys) {
            leaf.acquireReadLock();
            List<byte[]> tuples = leaf.searchRange(key, key);
            assertEquals(1, tuples.size());
        }

    }


    @Test
    public void testSearchRangeAllTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            leaf.insertKeyValue(key, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(min - 1, max + 1);
        assertEquals(numberOfTuples,  tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.searchRange(min, max);
        assertEquals(numberOfTuples,  tuples.size());
    }


    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            leaf.insertKeyValue(key, bytes);
        }

        Collections.sort(keys);

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(keys.get(300), keys.get(512));
        assertEquals(213, tuples.size());


        leaf.acquireReadLock();
        tuples = leaf.searchRange(keys.get(1022), keys.get(1023));
        assertEquals(2, tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.searchRange(keys.get(0), keys.get(1));
        assertEquals(2, tuples.size());
    }

    @Test
    public void testSearchRangeWithoutTuples() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            leaf.insertKeyValue(key, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(max + 1, Integer.MAX_VALUE);
        assertEquals(0, tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.searchRange(Integer.MIN_VALUE, min - 1);
        assertEquals(0, tuples.size());
    }



    @Test
    public void testInsertKeyValueWithoutDuplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            leaf.insertKeyValue(key, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(min, max);
        assertEquals(numberOfTuples, tuples.size());

    }

    @Test
    public void testInsertKeyValueWithDuplicateKeys() throws Exception, UnsupportedGenericException {
        int order = 1024;

        int numberOfTuples = 1024;

        BTreeLeafNode leaf = new BTreeLeafNode(order, new BytesCounter());

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
            leaf.insertKeyValue(key, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchAndGetTuples(randomKey);
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