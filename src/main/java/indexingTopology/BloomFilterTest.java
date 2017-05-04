package indexingTopology;

import com.google.common.hash.BloomFilter;

import indexingTopology.bloom.BloomFilterHandler;
import indexingTopology.util.texi.Car;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by john on 29/4/17.
 */
public class BloomFilterTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        BloomFilterHandler bloomFilterHandler = new BloomFilterHandler(0.01, 100);
        BloomFilter<Car> bfCar = bloomFilterHandler.createCarBF();
        Car car = new Car(1, 1.1, 2.2);
        Car car1 = new Car(2, 1.1, 3.3);
        bfCar.put(car);
        bloomFilterHandler.store();
        bfCar = bloomFilterHandler.load();
        System.out.println("might contain: " + bfCar.mightContain(car1));
        System.out.println("Bloom Filter Test succeeded!");
    }
}
