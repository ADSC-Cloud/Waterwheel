package indexingTopology;

import indexingTopology.bloom.BloomFilterHandler;
import indexingTopology.bloom.DataFunnel;

import com.google.common.hash.BloomFilter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

/**
 * Created by john on 29/4/17.
 */
public class BloomFilterTest {
    public static void main(String[] args) throws IOException, URISyntaxException {

        BloomFilterHandler bloomFilterHandler = new BloomFilterHandler("bf.dat");

        // create a bloom filter for type Double
        BloomFilter<Double> doubleBloomFilter = bloomFilterHandler.createDoubleBloomFilter();

        // create some Double objects and put them in bloom filter
        Random random = new Random();
        double[] inputArray = new double[100];

        for (int i = 0; i < 100; i++) {
            inputArray[i] = random.nextDouble();
            bloomFilterHandler.put(new Double(inputArray[i]));

//            //or put entry into bloom filter directly
//            doubleBF.put(new Double(inputArray[i]));
        }

        // write current bloom filter to HDFS
        bloomFilterHandler.store();

        // load bloom filter back from HDFS
        BloomFilter<Double> newDoubleBloomFilter = bloomFilterHandler.load(DataFunnel.getDoubleFunnel());

        // testing
        for (int i = 0; i < 100; i++) {
            System.out.println(i + "th entry: " + (doubleBloomFilter.mightContain(inputArray[i]) == newDoubleBloomFilter.mightContain(inputArray[i])));
            System.out.println("random test: " + newDoubleBloomFilter.mightContain(random.nextDouble()));
        }
    }
}