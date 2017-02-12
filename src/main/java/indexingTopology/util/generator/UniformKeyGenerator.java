package indexingTopology.util.generator;

import indexingTopology.util.generator.KeyGenerator;

import java.util.Random;

/**
 * Created by acelzj on 2/2/17.
 */
public class UniformKeyGenerator implements KeyGenerator {

    Random random;
    int seed = 1000;

    public UniformKeyGenerator() {
        random = new Random(seed);
    }

    @Override
    public double generate() {
        return random.nextDouble();
    }
}
