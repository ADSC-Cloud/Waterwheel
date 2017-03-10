package indexingTopology.util;

import indexingTopology.config.TopologyConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by acelzj on 12/17/16.
 */
public class BalancedPartition<T extends Number> implements Serializable{

    private T lowerBound;

    private T upperBound;

    private int numberOfPartitions;

    private Map<Integer, Integer> intervalToPartitionMapping;

    private boolean enableRecord = false;

    private Histogram histogram;

    public BalancedPartition(int numberOfPartitions, T lowerBound, T upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numberOfPartitions = numberOfPartitions;
        intervalToPartitionMapping = getBalancedPartitionPlan();
    }

    public BalancedPartition(int numberOfPartitions, T lowerBound, T upperBound, boolean enableRecord) {
        this(numberOfPartitions, lowerBound, upperBound);
        histogram = new Histogram();
        if (enableRecord) {
            setEnableRecord();
        }
    }


    public BalancedPartition(int numberOfPartitions, T lowerBound, T upperBound,
                             Map<Integer, Integer> intervalToPartitionMapping) {
        this(numberOfPartitions, lowerBound, upperBound, true);
        this.intervalToPartitionMapping.putAll(intervalToPartitionMapping);
    }

    public Map<Integer, Integer> getBalancedPartitionPlan() {
        Double distance = (upperBound.doubleValue() - lowerBound.doubleValue()) / numberOfPartitions;
        Double miniDistance = (upperBound.doubleValue() - lowerBound.doubleValue()) / TopologyConfig.NUMBER_OF_INTERVALS;
        Double keyRangeUpperBound = lowerBound.doubleValue() + distance;
        Double bound = lowerBound.doubleValue() + miniDistance;


        Map<Integer, Integer> intervalToPartitionMapping = new HashMap<>();
        int bin = 0;
        for (int i = 0; i < TopologyConfig.NUMBER_OF_INTERVALS; ++i) {
            intervalToPartitionMapping.put(i, bin);
            bound += miniDistance;
            if (bound > keyRangeUpperBound) {
                keyRangeUpperBound = keyRangeUpperBound + distance;
                ++bin;
                bin = Math.min(numberOfPartitions - 1, bin);
            }
        }

        return intervalToPartitionMapping;
    }

    public Map<Integer, Integer> getIntervalToPartitionMapping() {
        return this.intervalToPartitionMapping;
    }

    public int getIntervalId(T key) {
        Double distance = (upperBound.doubleValue() - lowerBound.doubleValue()) / TopologyConfig.NUMBER_OF_INTERVALS;

        Double startLowerBound = lowerBound.doubleValue() + distance;
        Double endUpperBound = upperBound.doubleValue() - distance;

        if (key.doubleValue() <= startLowerBound) {
            return 0;
        }

        if (key.doubleValue() > endUpperBound) {
            return TopologyConfig.NUMBER_OF_INTERVALS - 1;
        }

        if ((key.doubleValue() - startLowerBound) % distance == 0) {
            return (int) ((key.doubleValue() - startLowerBound) / distance);
        } else {
            return (int) ((key.doubleValue() - startLowerBound) / distance + 1);
        }
    }

    public int getPartitionId(T key) {
        return intervalToPartitionMapping.get(getIntervalId(key));
    }

    public void setEnableRecord() {
        enableRecord = true;
    }

    public void record(T key) {
        if (enableRecord) {
            int intervalId = getIntervalId(key);
            histogram.record(intervalId);
        }
    }

    public Histogram getIntervalDistribution() {
        histogram.setDefaultValueForAbsentKey(TopologyConfig.NUMBER_OF_INTERVALS);
        return histogram;
    }

    public void setIntervalToPartitionMapping(Map<Integer, Integer> intervalToPartitionMapping) {
        this.intervalToPartitionMapping.clear();
        this.intervalToPartitionMapping.putAll(intervalToPartitionMapping);
    }

    public void clearHistogram() {
        histogram.clear();
    }
}
