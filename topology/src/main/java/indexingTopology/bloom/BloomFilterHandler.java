package indexingTopology.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import indexingTopology.util.taxi.Car;

import java.io.*;
import java.net.URISyntaxException;


/**
 * Created by john on 30/4/17.
 */
public class BloomFilterHandler {
    private double fpp;
    private int expectedInsertions;

    private String localFileName;
    private BloomFilter bloomFilter;

    /**
     * instantiate a bloom filter handler with default fpp and insertion numbers
     * @param localFileName user-defined local file to/from which the bloom filter object will be written/read from/to HDFS
     */
    public BloomFilterHandler(String localFileName) {
        this.fpp = BloomFilterConfig.fpp;
        this.expectedInsertions = BloomFilterConfig.expectedInsertions;

        this.localFileName = localFileName;
    }

    /**
     * create a bloom filter for Boolean type with default parameters
     */
    public BloomFilter<Boolean> createBooleanBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getBooleanFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Boolean type with user-defined parameters
     */
    public BloomFilter<Boolean> createBooleanBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getBooleanFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Byte type with default parameters
     */
    public BloomFilter<Byte> createByteBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getByteFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Byte type with user-defined parameters
     */
    public BloomFilter<Byte> createByteBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getByteFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Character type with default parameters
     */
    public BloomFilter<Character> createCharacterBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getCharFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Character type with user-defined parameters
     */
    public BloomFilter<Character> createCharacterBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getCharFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Short type with default parameters
     */
    public BloomFilter<Short> createShortBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getShortFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Short type with user-defined parameters
     */
    public BloomFilter<Short> createShortBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getShortFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Integer type with default parameters
     */
    public BloomFilter<Integer> createIntegerBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getIntegerFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Integer type with user-defined parameters
     */
    public BloomFilter<Integer> createIntegerBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getIntegerFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Long type with default parameters
     */
    public BloomFilter<Long> createLongBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getLongFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Long type with user-defined parameters
     */
    public BloomFilter<Long> createLongBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getLongFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }


    /**
     * create a bloom filter for Float type with default parameters
     */
    public BloomFilter<Float> createFloatBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getFloatFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Float type with user-defined parameters
     */
    public BloomFilter<Float> createFloatBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getFloatFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }


    /**
     * create a bloom filter for Double type with default parameters
     */
    public BloomFilter<Double> createDoubleBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getDoubleFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Double type with user-defined parameters
     */
    public BloomFilter<Double> createDoubleBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getDoubleFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for String type with default parameters
     */
    public BloomFilter<String> createStringBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getStringFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for String type with user-defined parameters
     */
    public BloomFilter<String> createStringBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getStringFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Car type with default parameters
     */
    public BloomFilter<Car> createCarBloomFilter() {
        bloomFilter = BloomFilter.create(DataFunnel.getCarFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    /**
     * create a bloom filter for Car type entry with user-defined parameters
     * @param fpp
     * @param expectedInsertions
     * @return
     */
    public BloomFilter<Car> createCarBloomFilter(int expectedInsertions, double fpp) {
        bloomFilter = BloomFilter.create(DataFunnel.getCarFunnel(), expectedInsertions, fpp);
        return bloomFilter;
    }

    /**
     * insert an object t into bloom filter
     * @param t
     * @param <T>
     */
    public <T> void put(T t) {
        this.bloomFilter.put(t);
    }

    /**
     * check whether an object t is contained in the bloom filter
     * @param t
     * @param <T>
     * @return
     */
    public <T> boolean mightContain(T t) {
        return this.bloomFilter.mightContain(t);
    }

    /**
     * store current bloom filter object into HDFS
     * @throws IOException
     * @throws URISyntaxException
     */
    public void store() throws IOException, URISyntaxException {

        // writing to a local file before writing to HDFS, if file exist, overwrite it
        try (OutputStream os = new FileOutputStream(this.localFileName, false)) {
            this.bloomFilter.writeTo(os);
        }

        // writing to HDFS from local file
        BFHDFSHandler.writeHDFS(this.localFileName);
    }

    /**
     * load bloom filter from HDFS into memory
     * @param tFunnel specific Funnel object in type T in which this bloom filter is in
     * @param <T> type T in which this bloom filter is in
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    public <T> BloomFilter<T> load(Funnel<T> tFunnel) throws IOException, URISyntaxException {

        // read bloom filter object from HDFS to local file
        BFHDFSHandler.readHDFS(this.localFileName);
        InputStream is = new FileInputStream(this.localFileName);

        // reconstruct bloom filter from local file data
        this.bloomFilter = BloomFilter.readFrom(is, tFunnel);
        return this.bloomFilter;
    }
}