package indexingTopology.cache;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.index.BTreeLeafNode;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 11/29/16.
 */
public class LRUCacheTest {

    DataSchema schema = new DataSchema();
    public void setUp() {
        schema.addIntField("a1");
        schema.setPrimaryIndexField("a1");
    }

    private List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
            "date", "time", "latitude", "longitude"));
    private ArrayList valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
            Double.class, Double.class, Double.class, Double.class, Double.class));

    @Test
    public void testCacheStartsEmpty() {
        LRUCache<Integer, CacheUnit> cache = new LRUCache<Integer, CacheUnit>(2);
        assertEquals(cache.get(1), null);
    }

    @Test
    public void testCacheCapacityReachedOldestRemoved() throws UnsupportedGenericException, IOException {
        LRUCache<BlockId, CacheUnit> cache = new LRUCache<BlockId, CacheUnit>(2);

        CacheUnit cacheUnit = new CacheUnit();
        BTreeLeafNode leaf = new BTreeLeafNode(4);
        for (int i = 0; i < 4; ++i) {
//            List<Double> values = new ArrayList<>();
//            values.add((double) i);
//            for (int j = 0; j < fieldNames.size() + 1; ++j) {
//                values.add((double) j);
//            }
//            byte[] bytes = serializeIndexValue(values);
//            leaf.insertKeyTuples(i, bytes, false);
            DataTuple dataTuple = new DataTuple(i);
            leaf.insertKeyTuples(i, dataTuple, false);
        }
        CacheData data = new LeafNodeCacheData(leaf);
        cacheUnit.setCacheData(data);
        BlockId mappingKey0 = new BlockId("file0", 0);
        cache.put(mappingKey0, cacheUnit);
        assertEquals(leaf, cache.get(mappingKey0).getCacheData().getData());
//        ((LeafNodeCacheData) cache.get(0).getCacheData()).getData().print();

        cacheUnit = new CacheUnit();
        BTreeLeafNode leaf1 = new BTreeLeafNode(4);
        for (int i = 4; i < 8; ++i) {
//            List<Double> values = new ArrayList<>();
//            values.add((double) i);
//            for (int j = 0; j < fieldNames.size() + 1; ++j) {
//                values.add((double) j);
//            }
//            byte[] bytes = serializeIndexValue(values);
//            leaf1.insertKeyTuples(i, bytes, false);
            DataTuple dataTuple = new DataTuple(i);
            leaf1.insertKeyTuples(i, dataTuple, false);
        }
        data = new LeafNodeCacheData(leaf1);
        cacheUnit.setCacheData(data);
        BlockId mappingKey1 = new BlockId("file1", 1);
        cache.put(mappingKey1, cacheUnit);
        assertEquals(leaf1, cache.get(mappingKey1).getCacheData().getData());
//        ((LeafNodeCacheData) cache.get(1).getCacheData()).getData().print();

        cacheUnit = new CacheUnit();
        BTreeLeafNode leaf2 = new BTreeLeafNode(4);
        for (int i = 8; i < 12; ++i) {
//            List<Double> values = new ArrayList<>();
//            values.add((double) i);
//            for (int j = 0; j < fieldNames.size() + 1; ++j) {
//                values.add((double) j);
//            }
//            byte[] bytes = serializeIndexValue(values);
//            leaf2.insertKeyTuples(i, bytes, false);
            DataTuple dataTuple = new DataTuple(i);
            leaf2.insertKeyTuples(i, dataTuple, false);
        }
        data = new LeafNodeCacheData(leaf2);
        cacheUnit.setCacheData(data);
        BlockId mappingKey2 = new BlockId("file2", 2);
        cache.put(mappingKey2, cacheUnit);
        assertEquals(leaf1, cache.get(mappingKey1).getCacheData().getData());
        assertEquals(leaf2, cache.get(mappingKey2).getCacheData().getData());
        assertEquals(cache.get(mappingKey0), null);

        cacheUnit = new CacheUnit();
        data = new LeafNodeCacheData(leaf);
        cacheUnit.setCacheData(data);
        cache.put(mappingKey0, cacheUnit);
        assertEquals(leaf, cache.get(mappingKey0).getCacheData().getData());
        assertEquals(leaf2, cache.get(mappingKey2).getCacheData().getData());
        assertEquals(cache.get(mappingKey1), null);

        cache.get(mappingKey0).getCacheData().getData();
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