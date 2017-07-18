package indexingTopology.util;

import indexingTopology.common.Histogram;

import java.util.*;

public class RangePartitioner {

    private Map<Integer, Integer> ballToBinMapping = new HashMap<>();

    private Histogram histogram;

    private int nbins;

    private int numberOfBalls;

    public RangePartitioner(int numberOfBins, int numberOfBalls, Map<Integer, Integer> ballToBinMapping,
                            Histogram histogram) {
        this.ballToBinMapping.putAll(ballToBinMapping);
        this.histogram = histogram;
        nbins = numberOfBins;
        this.numberOfBalls = numberOfBalls;
    }

    public Map<Integer, Integer> getRepartitionPlan() {
        Long workload = getTotalWorkLoad(histogram.histogramToList());

        Double averWeight =  workload * 1.0 / nbins;


        List<Long> workloads = histogram.histogramToList();

        double accumulatedWeight = 0;
        Map<Integer, Integer> newBallToBinMapping = new HashMap<>();

        int bin = 0;
        long currentWeight = 0;
        for(int ball = 0; ball < numberOfBalls; ball++) {
            if (accumulatedWeight > averWeight) {
                bin++;
                bin = Math.min(bin, nbins - 1);
                accumulatedWeight -= currentWeight;
                accumulatedWeight -= averWeight - currentWeight;
                currentWeight = 0;
            }
            accumulatedWeight += workloads.get(ball);
            currentWeight += workloads.get(ball);
            newBallToBinMapping.put(ball, bin);
        }

        long[] loads = new long[nbins];

        for(int ball = 0; ball < numberOfBalls; ball++) {
            bin = newBallToBinMapping.get(ball);
            loads[bin] += workloads.get(ball);
        }

        return newBallToBinMapping;
    }


    public double getSkewnessFactor() {

        List<Long> workLoads = getWorkLoads();


        Long sum = getTotalWorkLoad(workLoads);
        Long maxWorkload = getMaxWorkLoad(workLoads);
        double averageLoad = sum / (double) nbins;

        return maxWorkload / averageLoad - 1;
    }


    public List<Long> getWorkLoads() {
        List<Long> workloads = new ArrayList<>();
        for (int i = 0; i < nbins; i++) {
            workloads.add(0L);
        }

        for (Integer ball: ballToBinMapping.keySet()) {
            int bin = ballToBinMapping.get(ball);
            long ballWorkload = histogram.getHistogram().get(ball);
            long binWorkload = workloads.get(bin);
            workloads.set(bin, binWorkload + ballWorkload);
        }
        return workloads;
    }

//    public Long getTotalWorkLoad(Histogram histogram) {
//        long ret = 0;
//
//        for(long i : histogram.histogramToList()) {
//            ret += i;
//        }
//
//        return ret;
//    }

    public Long getTotalWorkLoad(List<Long> workLoads) {
        long ret = 0;

        for(long i : workLoads) {
            ret += i;
        }

        return ret;
    }

//    private Long getMaxWorkLoad(Histogram histogram) {
//        long ret = Long.MIN_VALUE;
//        for(long i : histogram.histogramToList()) {
//            ret = Math.max(ret, i);
//        }
//        Map<Integer, Integer> intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();
//
//        int partitionId = 0;

    //        long tmpWorkload = 0;
//
//        List<Long> workLoads = histogram.histogramToList();
//
//        for (int intervalId = 0; intervalId < TopologyConfig.NUMBER_OF_INTERVALS; ++intervalId) {
//            if (intervalToPartitionMapping.get(intervalId) != partitionId) {
//                ret = Math.max(ret, tmpWorkload);
//                tmpWorkload = 0;
//                partitionId = intervalToPartitionMapping.get(intervalId);
//            }
//
//            tmpWorkload += workLoads.get(intervalId);
//        }
//
//        ret = Math.max(ret, tmpWorkload);
//        return ret;
//    }
    private Long getMaxWorkLoad(List<Long> workLoads) {
        long ret = 0;

        for(long i : workLoads) {
            ret = Math.max(ret, i);
        }

        return ret;
    }

    public static void main(String[] args) {
        Histogram histogram = new Histogram(6);
        histogram.record(0);
        histogram.record(0);
        histogram.record(1);
        histogram.record(1);
        histogram.record(1);
        histogram.record(2);
        histogram.record(3);
        histogram.record(4);
        histogram.record(5);
        Map<Integer, Integer> intervalToPartitionMapping = new HashMap<>();
        intervalToPartitionMapping.put(0, 0);
        intervalToPartitionMapping.put(1, 0);
        intervalToPartitionMapping.put(2, 0);

        intervalToPartitionMapping.put(3, 1);
        intervalToPartitionMapping.put(4, 1);
        intervalToPartitionMapping.put(5, 1);
        RangePartitioner manager = new RangePartitioner(2, 6, intervalToPartitionMapping, histogram);
        System.out.println(manager.getSkewnessFactor());
        System.out.println(manager.getWorkLoads());

        intervalToPartitionMapping = manager.getRepartitionPlan();
        manager = new RangePartitioner(2, 6, intervalToPartitionMapping, histogram);
        System.out.println(manager.getSkewnessFactor());
        System.out.println(manager.getWorkLoads());

    }
}
