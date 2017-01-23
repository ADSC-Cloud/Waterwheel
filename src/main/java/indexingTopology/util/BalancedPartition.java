package indexingTopology.util;

import indexingTopology.config.TopologyConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by acelzj on 12/17/16.
 */
public class BalancedPartition {

    private Double lowerBound;

    private Double upperBound;

    private int numberOfPartitions;

    private Map<Integer, Integer> intervalToPartitionMapping;

    private boolean enableRecord = false;

    private Histogram histogram;

    public BalancedPartition(int numberOfPartitions, Double lowerBound, Double upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numberOfPartitions = numberOfPartitions;
        intervalToPartitionMapping = getBalancedPartitionPlan();
    }

    public BalancedPartition(int numberOfPartitions, Double lowerBound, Double upperBound, boolean enableRecord) {
        this(numberOfPartitions, lowerBound, upperBound);
        histogram = new Histogram();
        if (enableRecord) {
            setEnableRecord();
        }
    }

    public BalancedPartition(int numberOfPartitions, Double lowerBound, Double upperBound,
                             Map<Integer, Integer> intervalToPartitionMapping) {
        this(numberOfPartitions, lowerBound, upperBound, true);
        this.intervalToPartitionMapping.putAll(intervalToPartitionMapping);
    }

    public Map<Integer, Integer> getBalancedPartitionPlan() {
        Double distance = (upperBound - lowerBound) / numberOfPartitions;
        Double miniDistance = (upperBound - lowerBound) / TopologyConfig.NUMBER_OF_INTERVALS;
        Double keyRangeUpperBound = lowerBound + distance;
        Double bound = lowerBound + miniDistance;
        Map<Integer, Integer> intervalToPartitionMapping = new HashMap<>();
        int bin = 0;
        for (int i = 0; i < TopologyConfig.NUMBER_OF_INTERVALS; ++i) {
            intervalToPartitionMapping.put(i, bin);
            bound += miniDistance;
            if (bound > keyRangeUpperBound) {
                keyRangeUpperBound = keyRangeUpperBound + distance;
                ++bin;
            }
        }
        return intervalToPartitionMapping;
    }

    public Map<Integer, Integer> getIntervalToPartitionMapping() {
        return this.intervalToPartitionMapping;
    }

    public int getIntervalId(Double key) {
        Double distance = (upperBound - lowerBound) / TopologyConfig.NUMBER_OF_INTERVALS;

        Double autualLowerBound = lowerBound + distance;

        Double autualUpperBound = upperBound - distance;

        if (key <= autualLowerBound) {
            return 0;
        }

        if (key > autualUpperBound) {
            return TopologyConfig.NUMBER_OF_INTERVALS - 1;
        }

        if ((key - autualLowerBound) % distance == 0) {
            return (int) ((key - autualLowerBound) / distance);
        } else {
            return (int) ((key - autualLowerBound) / distance + 1);
        }
    }

    public int getPartitionId(Double key) {
        return intervalToPartitionMapping.get(getIntervalId(key));
    }

    public int getPartitionId(Integer intervalId) {
        return intervalToPartitionMapping.get(intervalId);
    }

    public void setEnableRecord() {
        enableRecord = true;
    }

    public void record(Double key) {
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
