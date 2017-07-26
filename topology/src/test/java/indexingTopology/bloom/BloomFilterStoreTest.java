package indexingTopology.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import indexingTopology.config.TopologyConfig;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Robert on 6/16/17.
 */
public class BloomFilterStoreTest extends TestCase{
    TopologyConfig config = new TopologyConfig();
    public void setUp() {
        try {
            Runtime.getRuntime().exec("mkdir -p ./target/tmp");
        } catch (IOException e) {
            e.printStackTrace();
        }
        config.HDFSFlag = false;
        config.dataChunkDir = "./target/tmp";
        config.metadataDir = "./target/tmp";
    }

    public void tearDown() {
        try {
            Runtime.getRuntime().exec("rm -rf ./target/tmp");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStoreAndRead() throws IOException {
        BloomFilterStore store = new BloomFilterStore(config);
        BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), 30000);
        bloomFilter.put(10L);
        bloomFilter.put(13L);
        BloomFilterStore.BloomFilterId id = new BloomFilterStore.BloomFilterId("chunk 1", "a1");
        store.store(id, bloomFilter);
        BloomFilter filter1 = store.get(new BloomFilterStore.BloomFilterId("chunk 1", "a1"));
        assertEquals(true, filter1.mightContain(10L));
        assertEquals(true, filter1.mightContain(13L));
    }
}
