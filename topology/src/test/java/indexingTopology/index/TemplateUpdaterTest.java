package indexingTopology.index;

import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import indexingTopology.exception.UnsupportedGenericException;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by acelzj on 12/21/16.
 */
public class TemplateUpdaterTest {

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));
    private DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id","time");

    private TopologyConfig config = new TopologyConfig();

    @Test
    public void testCreateTreeWithBulkLoading() throws Exception, UnsupportedGenericException {

//        int numberOfTuples = 64;
        int numberOfTuples = (int)(config.CHUNK_SIZE / (8 * 8) * 1.4);

//        int order = 4;
        int order = config.BTREE_ORDER;

        BTree bTree = new BTree(order, config);

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

//        bTree.printBtree();

        TemplateUpdater templateUpdater = new TemplateUpdater(order, config);

        Long start = System.currentTimeMillis();
        BTree newTree = templateUpdater.createTreeWithBulkLoading(bTree);


//        newTree.printBtree();

//        for (Integer key : keys) {
//            assertEquals(1, newTree.searchRange(key, key).size());
//        }
        int total = 0;
        BTreeLeafNode leaf = newTree.getLeftMostLeaf();

        while (leaf != null) {
//            leaf.print();
//            System.out.println(leaf.atomicKeyCount.get());
            total += leaf.getAtomicKeyCount();
            assertEquals(leaf.getKeyCount(), leaf.getAtomicKeyCount());
            leaf = (BTreeLeafNode) leaf.rightSibling;
        }


        assertEquals(total, numberOfTuples);
    }

    @Test
    public void testCreateTreeWithBulkLoadingWithDuplicatedTuples() throws Exception, UnsupportedGenericException {

        int numberOfTuples = (int)(config.CHUNK_SIZE / (8 * 8) * 1.4);
//        int numberOfTuples = TopologyConfig.NUMBER_TUPLES_OF_A_CHUNK;

        int order = 4;
//        int order = TopologyConfig.BTREE_ORDER;

        BTree bTree = new BTree(order, config);

        Random random = new Random();

        List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < numberOfTuples; ++i) {
//            Integer key = random.nextInt();
//            while (keys.contains(key)) {
//                key = random.nextInt();
//            }
            keys.add(i);
        }

        Collections.shuffle(keys);

        int duplicatedTime = 5;

        for (Integer key : keys) {
            List<Double> values = new ArrayList<>();
            values.add((double) key);
            for (int j = 0; j < fieldNames.size() + 1; ++j) {
                values.add((double) j);
            }
            byte[] bytes = serializeIndexValue(values);
            for (int i = 0; i < duplicatedTime; ++i) {
                bTree.insert(key*1.0, bytes);
            }
        }


//        bTree.printBtree();

        TemplateUpdater templateUpdater = new TemplateUpdater(order, config);

//        Long start = System.currentTimeMillis();
        BTree newTree = templateUpdater.createTreeWithBulkLoading(bTree);
//        System.out.println((System.currentTimeMillis() - start) / 1000);

//        newTree.printBtree();

        for (Integer key : keys) {
            assertEquals(duplicatedTime, newTree.searchRange(key*1.0, key*1.0).size());
        }

        BTreeLeafNode leaf = newTree.getLeftMostLeaf();
        while (leaf != null) {
//            leaf.print();
//            System.out.println(leaf.atomicKeyCount.get());
            assertEquals(duplicatedTime * leaf.getKeyCount(), leaf.getAtomicKeyCount());
            leaf = (BTreeLeafNode) leaf.rightSibling;
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