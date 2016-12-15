package indexingTopology.util;

import indexingTopology.Config.Config;

/**
 * Created by acelzj on 12/13/16.
 */
public class IntervalIdMappingFunction {

    public static int getIntervalId(Double key, Double lowerBound, Double upperBound) {
        int distance = (int) (upperBound - lowerBound) / Config.NUMBER_OF_INTERVALS;

        Double autualLowerBound = lowerBound + distance;

        Double autualUpperBound = upperBound - distance;

        if (key <= autualLowerBound) {
            return 0;
        }

        if (key > autualUpperBound) {
            return Config.NUMBER_OF_INTERVALS - 1;
        }

        if ((key - autualLowerBound) % distance == 0) {
            return (int) (key - autualLowerBound) / distance;
        } else {
            return (int) ((key - autualLowerBound) / distance + 1);
        }
    }

}
