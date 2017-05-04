package indexingTopology.bloom;

import com.google.common.hash.BloomFilter;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;


/**
 * Created by john on 30/4/17.
 */
public class BloomFilterHandler {

    private long count = 0L;
    private double fpp;
    private int expectedInsertions;

    private String fileName = "bf.dat";
    private BloomFilter bloomFilter;

    public BloomFilterHandler(double fpp, int expectedInsertions) {
        this.fpp = BloomFilterConfig.fpp;
        this.expectedInsertions = BloomFilterConfig.expectedInsertions;
    }

    public BloomFilter<Car> createCarBF() {
        bloomFilter = BloomFilter.create(DataFunnel.getCarFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    public BloomFilter<City> createCityBF() {
        bloomFilter = BloomFilter.create(DataFunnel.getCityFunnel(), this.expectedInsertions, this.fpp);
        return bloomFilter;
    }

    public <T> void insert(T t) {
        this.bloomFilter.put(t);
    }


    public <T> boolean mightContain(T t) {
        return this.bloomFilter.mightContain(t);
    }


    /**
     * Save Bloom Filter to HDFS files
     */
    public void store() throws IOException, URISyntaxException {
        FileOutputStream fos = new FileOutputStream(fileName);
        this.bloomFilter.writeTo(fos);
        BFHDFSHandler.writeHDFS(fileName);
    }

    /**
     * Restore Bloom Filter from HDFS files
     */

    public BloomFilter<Car> load() throws IOException, URISyntaxException {

        BFHDFSHandler.readHDFS(fileName);

        FileInputStream fis = new FileInputStream(fileName);

        this.bloomFilter = BloomFilter.readFrom(fis, DataFunnel.getCarFunnel());
        return bloomFilter;
    }
}
