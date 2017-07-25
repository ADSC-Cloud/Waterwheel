package indexingTopology.util.partition;

import indexingTopology.common.Histogram;

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

    private int numberOfIntervals;

    private Map<Integer, Integer> intervalToPartitionMapping;

    private boolean enableRecord = true;

    private Histogram histogram;


    public BalancedPartition(int numberOfPartitions, int numberOfIntervals, T lowerBound, T upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numberOfPartitions = numberOfPartitions;
        this.numberOfIntervals = numberOfIntervals;
        intervalToPartitionMapping = getBalancedPartitionPlan();
    }

    public BalancedPartition(int numberOfPartitions, int numberofIntervals, T lowerBound, T upperBound, boolean enableRecord) {
        this(numberOfPartitions, numberofIntervals, lowerBound, upperBound);
        histogram = new Histogram(numberofIntervals);
        if (enableRecord) {
            setEnableRecord();
        }
    }


    public BalancedPartition(int numberOfPartitions, int numberOfIntervals, T lowerBound, T upperBound,
                             Map<Integer, Integer> intervalToPartitionMapping) {
        this(numberOfPartitions, numberOfIntervals, lowerBound, upperBound, true);
        this.intervalToPartitionMapping.putAll(intervalToPartitionMapping);
    }

    public Map<Integer, Integer> getBalancedPartitionPlan() {
        Double distance = (upperBound.doubleValue() - lowerBound.doubleValue()) / numberOfPartitions;
        Double miniDistance = (upperBound.doubleValue() - lowerBound.doubleValue()) / numberOfIntervals;
        Double keyRangeUpperBound = lowerBound.doubleValue() + distance;
        Double bound = lowerBound.doubleValue() + miniDistance;


        Map<Integer, Integer> intervalToPartitionMapping = new HashMap<>();
        int bin = 0;
        for (int i = 0; i < numberOfIntervals; ++i) {
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
//        Double distance = (upperBound.doubleValue() - lowerBound.doubleValue()) / numberOfIntervals;
//
//        Double startLowerBound = lowerBound.doubleValue() + distance;
//        Double endUpperBound = upperBound.doubleValue() - distance;
//
//        if (key.doubleValue() <= startLowerBound) {
//            return 0;
//        }
//
//        if (key.doubleValue() > endUpperBound) {
//            return numberOfIntervals - 1;
//        }
//
//        if ((key.doubleValue() - startLowerBound) % distance == 0) {
//            return (int) ((key.doubleValue() - startLowerBound) / distance);
//        } else {
//            return (int) ((key.doubleValue() - startLowerBound) / distance + 1);
//        }
        if (key.doubleValue() < lowerBound.doubleValue()) {
            return 0;
        }

        if (key.doubleValue() >= upperBound.doubleValue()) {
            return numberOfIntervals - 1;
        }

        Double distance = (upperBound.doubleValue() - lowerBound.doubleValue()) / numberOfIntervals;
        return (int)((key.doubleValue() - lowerBound.doubleValue()) / distance);
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
        histogram.setDefaultValueForAbsentKey(numberOfIntervals);
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
