package indexingTopology.util.taxi;

import java.io.Serializable;

/**
 * Created by Robert on 12/10/16.
 */
public class RangePartition implements Serializable {
    int numberOfPartition;
    Number max;
    Number min;
    private Number wide;

    public RangePartition(int numberOfPartitions, Number min, Number max) {
        if(numberOfPartitions < 0) {
            throw new IllegalArgumentException(String.format("numberOfPartition should be positive!"));
        }
        if(min.doubleValue() >= max.doubleValue()) {
            throw new IllegalArgumentException("min should smaller than max!");
        }

        this.numberOfPartition = numberOfPartitions;
        this.min = min;
        this.max = max;
        this.wide = max.doubleValue() - min.doubleValue();
    }

    public int  getPartition(Number key) {
        int ret = (int)((key.doubleValue() - min.doubleValue()) / wide.doubleValue() * numberOfPartition);
        ret = Math.min(ret, numberOfPartition - 1);
        ret = Math.max(ret, 0);
        return ret;
    }
}
