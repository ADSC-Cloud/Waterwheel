package indexingTopology.util;

import indexingTopology.config.TopologyConfig;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class RepartitionManager {

    private Map<Integer, Integer> ballToBinMapping = new HashMap<>();

    private Histogram histogram;

    private int nbins;

    private int numberOfBalls;

    public RepartitionManager(int numberOfBins, int numberOfBalls, Map<Integer, Integer> ballToBinMapping,
                              Histogram histogram) {
        this.ballToBinMapping.putAll(ballToBinMapping);
        this.histogram = histogram;
        nbins = numberOfBins;
        this.numberOfBalls = numberOfBalls;
    }

    public Map<Integer, Integer> getRepartitionPlan() {
        Long workload = getTotalWorkLoad(histogram.histogramToList());

        Double averWeight =  workload * 1.0 / nbins;

//        Object[] balls = ballToBinMapping.keySet().toArray();

        List<Integer> balls = new ArrayList<>(ballToBinMapping.keySet());

        Collections.sort(balls);
//        System.out.println(ballToBinMapping);
//        System.out.println(ballToWeight);

        List<Long> workloads = histogram.histogramToList();

        long[] loadsBeforeRepartition = new long[nbins];

        for (Integer ball : balls) {
            int bin = ballToBinMapping.get(ball);
            loadsBeforeRepartition[bin % nbins] += workloads.get(ball);
        }
//
//        for (int i = 0; i < loadsBeforeRepartition.length; ++i) {
//            System.out.println("bin " + i + " : " + loadsBeforeRepartition[i]);
//        }



        double accumulatedWeight = 0;
        Map<Integer, Integer> newBallToBinMapping = new HashMap<>();

        int bin = 0;
        long currentWeight = 0;
        for(Integer ball: balls) {
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

//        System.out.println(newBallToBinMapping);


//        Map<Integer, Integer> newBallToBinMapping = new HashMap<>();
//        Long currentWeight = 0L;
//        Double differ = 0.0;
//        int bin = 0;
//        for (Object ball : balls) {
//            Long weight = workloads.get((int) ball);
//            Long weightBeforeAdded = currentWeight;
//            currentWeight += weight;
//            newBallToBinMapping.put((Integer) ball, bin);
////            differ += weight;
//            if (currentWeight + differ >= averWeight) {
//                differ = currentWeight - averWeight;
//                ++bin;
//                bin = Math.min(bin, nbins - 1);
//                if (Math.abs(weightBeforeAdded - averWeight) < Math.abs(weight - averWeight)) {
//                    newBallToBinMapping.put((Integer) ball, bin);
//                    differ -= weight;
//                    currentWeight = weight;
//                } else {
//                    currentWeight = 0L;
//                }
//
//            }
//        }



        long[] loads = new long[nbins];

//        System.out.println(newBallToBinMapping);

        for (Object ball : balls) {
            bin = newBallToBinMapping.get(ball);
            loads[bin % nbins] += workloads.get((int) ball);
        }

//        System.out.println("Repartition has been finished!");
//        for (int i = 0; i < loads.length; ++i) {
//            System.out.println("bin " + i + " : " + loads[i]);
//        }

        return newBallToBinMapping;
    }


    public double getSkewnessFactor() {

        List<Long> workLoads = getWorkLoads();

//        System.out.println("workloads " + workLoads);

        Long sum = getTotalWorkLoad(workLoads);
//        Long sum = getTotalWorkLoad(histogram);
//        Long maxWorkload = getMaxWorkLoad(histogram);
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
        RepartitionManager manager = new RepartitionManager(2, 6, intervalToPartitionMapping, histogram);
        System.out.println(manager.getSkewnessFactor());
        System.out.println(manager.getWorkLoads());

        intervalToPartitionMapping = manager.getRepartitionPlan();
        manager = new RepartitionManager(2, 6, intervalToPartitionMapping, histogram);
        System.out.println(manager.getSkewnessFactor());
        System.out.println(manager.getWorkLoads());

    }
}
