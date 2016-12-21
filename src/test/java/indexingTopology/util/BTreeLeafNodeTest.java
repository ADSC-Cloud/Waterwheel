package indexingTopology.util;

import org.apache.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());
        BTreeNode root = null;

        for (int i = 0; i < 5; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            root = leaf.insertKeyValue(i, bytes);
        }

        assertEquals(2, leaf.getKeyCount());
        assertEquals(3, leaf.rightSibling.getKeyCount());
        assertEquals(1, root.getKeyCount());
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        for (int i = 0; i < 4; ++i) {
            leaf.acquireReadLock();
            List<byte[]> tuples = leaf.searchRange(0, 0);
            assertEquals(1, tuples.size());
        }

    }


    @Test
    public void testSearchRangeAllTuples() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(0, 5);
        assertEquals(4, tuples.size());

    }


    @Test
    public void testSearchRangeSomeTuples() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(1, 3);
        assertEquals(3, tuples.size());

    }

    @Test
    public void testSearchRangeWithoutTuples() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        leaf.acquireReadLock();
        List<byte[]> tuples = leaf.searchRange(4, 8);
        assertEquals(0, tuples.size());

        leaf.acquireReadLock();
        tuples = leaf.searchRange(-5, -1);
        assertEquals(0, tuples.size());
    }

    @Test
    public void testSearchAndGetTuplesWithinTimestampRange() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            System.out.println(values);
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        for (int i = 0; i < 4; ++i) {
            assertEquals(1, leaf.searchAndGetTuplesWithinTimestampRange(i, 4620693217682128896L, 4620693217682128896L).size());
        }

        for (int i = 0; i < 4; ++i) {
            assertEquals(1, leaf.searchAndGetTuplesWithinTimestampRange(i, 4620693217682128892L, 4620693217682128899L).size());
        }
    }

    @Test
    public void testSearchAndGetTuplesNotinTimestampRange() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            System.out.println(values);
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        for (int i = 0; i < 4; ++i) {
            assertEquals(0, leaf.searchAndGetTuplesWithinTimestampRange(i, 0L, 1L).size());
        }
    }

    @Test
    public void testInsertKeyValueWithoutDuplicateKey() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());

        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(i, bytes);
        }

        for (int i = 0; i < 4; ++i) {
            ArrayList<byte[]> bytes = leaf.searchAndGetTuples(i);
            for (int j = 0; j < bytes.size(); ++j) {
                System.out.println(DeserializationHelper.deserialize(bytes.get(j)));
            }
        }
    }

    @Test
    public void testInsertKeyValueWithDuplicateKey() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                "date", "time", "latitude", "longitude"));
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");
        Double key = 0.0;
        for (int i = 0; i < 4; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            leaf.insertKeyValue(key, bytes);
        }
        ArrayList<byte[]> bytes = leaf.searchAndGetTuples(key);
        for (int j = 0; j < bytes.size(); ++j) {
            System.out.println(DeserializationHelper.deserialize(bytes.get(j)));
        }
        assertEquals(4, bytes.size());

    }



    private void writeToByteArrayOutputStream(ByteArrayOutputStream bos, byte[] b) {
        try {
            bos.write(b);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Double> getValuesObject(String [] valuesAsString) throws IOException {

        List<Double> values = new ArrayList<Double>();
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        for (int i=0;i < valueTypes.size();i++) {
            if (valueTypes.get(i).equals(Double.class)) {
                values.add(Double.parseDouble(valuesAsString[i]));
            }
        }
        return values;
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