package indexingTopology.util.generator;


import indexingTopology.util.Permutation;
import indexingTopology.util.generator.KeyGenerator;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.storm.utils.Utils;

/**
 * Created by acelzj on 2/2/17.
 */
public class ZipfKeyGenerator implements KeyGenerator {

    ZipfDistribution distribution;
    RandomGenerator generator;
    Permutation permutation;
    int numberOfKeys;

    public ZipfKeyGenerator(int numberOfKeys, double skewness, RandomGenerator generator) {
        this.generator = generator;
        this.numberOfKeys = numberOfKeys;
        distribution = new ZipfDistribution(generator, numberOfKeys, skewness);
        permutation = new Permutation(numberOfKeys);
        Thread changeDistributionThread = new Thread(new ChangeDistributionRunnable());
        changeDistributionThread.start();
    }

    @Override
    public double generate() {
        int key = distribution.sample() - 1; // minus 1 is to ensure that the key starting from 0 instead of 1.
        return permutation.get(key);
    }


    class ChangeDistributionRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                Utils.sleep(500 * 2);
//                distribution = new ZipfDistribution(numberOfKeys, generator.nextDouble());
//                distribution = new ZipfDistribution(numberOfKeys, generator.nextDouble());
                permutation.shuffle(100);
            }
        }
    }
}
