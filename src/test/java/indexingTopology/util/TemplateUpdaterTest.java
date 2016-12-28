package indexingTopology.util;

import indexingTopology.DataSchema;
import indexingTopology.exception.UnsupportedGenericException;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/21/16.
 */
public class TemplateUpdaterTest {

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

    @Test
    public void testCreateTreeWithBulkLoading() throws Exception, UnsupportedGenericException {

        int numberOfTuples = 2048;

        int order = 128;

        BTree bTree = new BTree(order, TimingModule.createNew(), SplitCounterModule.createNew());

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
            Integer key = random.nextInt();
            while (keys.contains(key)) {
                key = random.nextInt();
            }
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

        TemplateUpdater templateUpdater = new TemplateUpdater(4, TimingModule.createNew(), SplitCounterModule.createNew());

        BTree newTree = templateUpdater.createTreeWithBulkLoading(bTree);

        for (Integer key : keys) {
            assertEquals(1, newTree.searchTuples(key).size());
        }

    }

    public byte[] serializeIndexValue(List<Double> values) throws IOException {
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