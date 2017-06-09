package indexingTopology.util.generator;

import indexingTopology.util.generator.KeyGenerator;

/**
 * Created by acelzj on 8/2/17.
 */
public class RoundRobinKeyGenerator implements KeyGenerator {

    int numberOfKeys;
    int seed = 0;

    public RoundRobinKeyGenerator(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    @Override
    public double generate() {
        seed = (seed + 1) % numberOfKeys;
        return seed;
    }
}
