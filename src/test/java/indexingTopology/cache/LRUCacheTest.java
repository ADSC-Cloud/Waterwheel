package indexingTopology.cache;

import indexingTopology.exception.UnsupportedGenericException;
import indexingTopology.util.*;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 11/29/16.
 */
public class LRUCacheTest {

    @Test
    public void testCacheStartsEmpty() {
        LRUCache<Integer, CacheUnit> cache = new LRUCache<Integer, CacheUnit>(2);
        assertEquals(cache.get(1), null);
    }

    @Test
    public void testCacheCapacityReachedOldestRemoved() throws UnsupportedGenericException {
        LRUCache<CacheMappingKey, CacheUnit> cache = new LRUCache<CacheMappingKey, CacheUnit>(2);

        CacheUnit cacheUnit = new CacheUnit();
        TimingModule tm = TimingModule.createNew();
        SplitCounterModule sm = SplitCounterModule.createNew();
        BytesCounter counter = new BytesCounter();
        BTreeLeafNode leaf = new BTreeLeafNode(4, counter);
        for (int i = 0; i < 4; ++i) {
            leaf.insertKeyValue(i, i);
        }
        CacheData data = new LeafNodeCacheData(leaf);
        cacheUnit.setCacheData(data);
        CacheMappingKey mappingKey0 = new CacheMappingKey("file0", 0);
        cache.put(mappingKey0, cacheUnit);
        assertEquals(leaf, cache.get(mappingKey0).getCacheData().getData());
//        ((LeafNodeCacheData) cache.get(0).getCacheData()).getData().print();

        cacheUnit = new CacheUnit();
        BTreeLeafNode leaf1 = new BTreeLeafNode(4, counter);
        for (int i = 4; i < 8; ++i) {
            leaf1.insertKeyValue(i, i);
        }
        data = new LeafNodeCacheData(leaf1);
        cacheUnit.setCacheData(data);
        CacheMappingKey mappingKey1 = new CacheMappingKey("file1", 1);
        cache.put(mappingKey1, cacheUnit);
        assertEquals(leaf1, cache.get(mappingKey1).getCacheData().getData());
//        ((LeafNodeCacheData) cache.get(1).getCacheData()).getData().print();

        cacheUnit = new CacheUnit();
        BTreeLeafNode leaf2 = new BTreeLeafNode(4, counter);
        for (int i = 8; i < 12; ++i) {
            leaf2.insertKeyValue(i, i);
        }
        data = new LeafNodeCacheData(leaf2);
        cacheUnit.setCacheData(data);
        CacheMappingKey mappingKey2 = new CacheMappingKey("file2", 2);
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

}