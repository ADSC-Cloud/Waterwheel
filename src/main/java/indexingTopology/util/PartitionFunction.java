package indexingTopology.util;

import indexingTopology.Config.Config;

/**
 * Created by acelzj on 12/13/16.
 */
public class PartitionFunction {

    public final Double lowerBound;

    public final Double upperBound;

    public PartitionFunction(Double lowerBound, Double upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public int getIntervalId(Double key) {
        Double distance = (upperBound - lowerBound) / Config.NUMBER_OF_INTERVALS;

        Double autualLowerBound = lowerBound + distance;

        Double autualUpperBound = upperBound - distance;

        if (key <= autualLowerBound) {
            return 0;
        }

        if (key > autualUpperBound) {
            return Config.NUMBER_OF_INTERVALS - 1;
        }

        if ((key - autualLowerBound) % distance == 0) {
            return (int) ((key - autualLowerBound) / distance);
        } else {
            return (int) ((key - autualLowerBound) / distance + 1);
        }
    }

}
