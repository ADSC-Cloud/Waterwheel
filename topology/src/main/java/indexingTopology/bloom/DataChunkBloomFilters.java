package indexingTopology.bloom;

/**
 * Created by robert on 9/5/17.
 */

import com.google.common.hash.BloomFilter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class DataChunkBloomFilters implements Serializable {
    public String chunkName;
    public Map<String, BloomFilter> columnToBloomFilter;

    public DataChunkBloomFilters(String chunkName) {
        this.chunkName = chunkName;
        columnToBloomFilter = new HashMap<>();
    }

    public void addBloomFilter(String column, BloomFilter filter) {
        columnToBloomFilter.put(column, filter);
    }

    public boolean mightContain(String column, Object element) {
        return columnToBloomFilter.get(column).mightContain(element);
    }
}
