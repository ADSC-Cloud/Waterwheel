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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        BTree bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
        }

        for (int i = 0; i < 10; ++i) {
            assertEquals(1, bTree.searchTuples(1).size());
        }

        //Test template mode
        bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        bTree.setTemplateMode();

        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
        }

        for (int i = 0; i < 10; ++i) {
            List<byte[]> bytes = bTree.searchTuples(i);
            assertEquals(1, bTree.searchTuples(i).size());
            for (int j = 0; j < bytes.size(); ++j) {
                System.out.println(DeserializationHelper.deserialize(bytes.get(j)));
            }
        }
    }

    @Test
    public void testSearchRangeLeftKeyAndRightKeyTheSame() throws Exception, UnsupportedGenericException {
        BTree bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
        }

        for (int i = 0; i < 10; ++i) {
            List<byte[]> bytes = bTree.searchRange(i, i);
            assertEquals(1, bytes.size());
            for (int j = 0; j < bytes.size(); ++j) {
                System.out.println(DeserializationHelper.deserialize(bytes.get(j)));
            }
        }
    }


    @Test
    public void testSearchRangeLeftKeyAndRightAllTuples() throws Exception, UnsupportedGenericException {
        BTree bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
        }

        List<byte[]> bytes = bTree.searchRange(-1, 20);
        assertEquals(10, bytes.size());

    }

    @Test
    public void testSearchRangeLeftKeyAndRightSomeTuples() throws Exception, UnsupportedGenericException {
        BTree bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
        }

        List<byte[]> bytes = bTree.searchRange(3, 8);
        assertEquals(6, bytes.size());

    }


    @Test
    public void clearPayload() throws Exception, UnsupportedGenericException {
        BTree bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
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
        BTree bTree = new BTree(4, TimingModule.createNew(), SplitCounterModule.createNew());
        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
        }

        bTree.clearPayload();

        for (int i = 0; i < 10; ++i) {
            List<Double> values = new ArrayList<>();
            values.add((double) i);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            bTree.insert(i, bytes);
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