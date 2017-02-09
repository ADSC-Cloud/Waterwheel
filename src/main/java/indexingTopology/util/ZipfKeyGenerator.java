package indexingTopology.util;


import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.storm.utils.Utils;

/**
 * Created by acelzj on 2/2/17.
 */
public class ZipfKeyGenerator implements KeyGenerator{

    ZipfDistribution distribution;
    RandomGenerator generator;
    int numberOfKeys;

    public ZipfKeyGenerator(int numberOfKeys, double skewness, RandomGenerator generator) {
        this.generator = generator;
        this.numberOfKeys = numberOfKeys;
        distribution = new ZipfDistribution(generator, numberOfKeys, skewness);
        Thread changeDistributionThread = new Thread(new ChangeDistributionRunnable());
        changeDistributionThread.start();
    }

    @Override
    public double generate() {
        return distribution.sample() - 1; // minus 1 is to ensure that the key starting from 0 instead of 1.
    }

    class ChangeDistributionRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                Utils.sleep(500);
                distribution = new ZipfDistribution(numberOfKeys, generator.nextDouble());
            }
        }
    }
}
