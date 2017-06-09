package indexingTopology.bloom;

/**
 * Created by john on 30/4/17.
 *
 * default Bloom Filter configurations:
 * 1. expectedInsertions: the number of entries that will be inserted into bloom filter. Once
 * the number of entries exceeds it, a new filter should be created to guarantee accuracy;
 * 2. fpp: false positive probability, a value denoting the accuracy.
 */
public class BloomFilterConfig {
    public static final int expectedInsertions = 1024 * 1024;
    public static final double fpp = 0.03;
}
