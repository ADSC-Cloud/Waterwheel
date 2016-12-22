package indexingTopology.util;

import org.apache.storm.tuple.Values;
import indexingTopology.Config.TopologyConfig;
import indexingTopology.DataSchema;
import indexingTopology.FileSystemHandler.FileSystemHandler;
import indexingTopology.FileSystemHandler.HdfsFileSystemHandler;
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

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

    @Test
    public void testSearchTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

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
            assertEquals(1, bTree.searchTuples(key).size());
        }

        //Test template mode
        BulkLoader bulkLoader = new BulkLoader(order, TimingModule.createNew(), SplitCounterModule.createNew());
        BTree newBTree = bulkLoader.createTreeWithBulkLoading(bTree);

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            newBTree.insert(key, bytes);
        }

        for (Integer key : keys) {
            assertEquals(1, bTree.searchTuples(key).size());
        }
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

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
    }


    @Test
    public void testSearchRangeLeftKeyAndRightAllTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

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

    }

    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        int order = 32;
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

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
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

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
        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

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