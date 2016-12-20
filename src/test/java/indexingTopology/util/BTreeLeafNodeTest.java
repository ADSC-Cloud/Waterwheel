package indexingTopology.util;

import backtype.storm.tuple.Values;
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
    @Test
    public void insertKeyValueWithoutDuplicateKey() throws Exception, UnsupportedGenericException {
        BTreeLeafNode leaf = new BTreeLeafNode(4, new BytesCounter());
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                "date", "time", "latitude", "longitude"));
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");
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
    public void insertKeyValueWithDuplicateKey() throws Exception, UnsupportedGenericException {
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

    /*
    @Test
    public void serialize() throws Exception {
        File inputFile = new File("src/input_data_new");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
        String text = null;
        SplitCounterModule sm = SplitCounterModule.createNew();
        TimingModule tm = TimingModule.createNew();
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = new BTreeLeafNode(4, counter);
        List<Double> values = null;
        Double indexValue = 0.0;
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                "date", "time", "latitude", "longitude"));
        ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        DataSchema schema = new DataSchema(fieldNames, valueTypes);
//        LinkedBlockingQueue<Pair> queue = new LinkedBlockingQueue<Pair>();
        for (int i = 0; i < 4; ++i) {
            try {
                text = bufferedReader.readLine();
                String[] tokens = text.split(" ");
                values = getValuesObject(tokens);
                indexValue = values.get(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byte[] serializedTuple = serializeIndexValue(values);
            System.out.println(serializedTuple.length);
            try {
                leaf.insertKeyValue(indexValue, serializedTuple);
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Leave: ");
        leaf.print();
        System.out.println(leaf.bytesCount);
        byte[] serializedLeave = leaf.serialize();
        BTreeLeafNode deserializedLeave = leaf.deserialize(serializedLeave, 4, counter);
        deserializedLeave.print();
        for (int i = 0; i < deserializedLeave.tuples.size(); ++i) {
            ArrayList<byte[]> tuples = (ArrayList<byte[]>) deserializedLeave.tuples.get(i);
            for (int j = 0; j < tuples.size(); ++j) {
                byte[] serializedTuple = tuples.get(j);
                Values value = schema.deserialize(serializedTuple);
                System.out.println(value);
            }
        }

    }*/




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