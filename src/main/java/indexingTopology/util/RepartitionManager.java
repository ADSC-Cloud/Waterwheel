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

    public RepartitionManager(int numberOfBins, Map<Integer, Integer> ballToBinMapping,
                              Histogram histogram) {
        this.ballToBinMapping.putAll(ballToBinMapping);
        this.histogram = histogram;
        nbins = numberOfBins;
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

        System.out.println("workloads " + workLoads);

        Long sum = getTotalWorkLoad(workLoads);
//        Long sum = getTotalWorkLoad(histogram);
//        Long maxWorkload = getMaxWorkLoad(histogram);
        Long maxWorkload = getMaxWorkLoad(workLoads);
        double averageLoad = sum / (double) nbins;

        return maxWorkload / averageLoad;
    }


    public List<Long> getWorkLoads() {
        List<Long> wordLoads = new ArrayList<>();

        int partitionId = 0;

        long tmpWorkload = 0;

        List<Long> workLoads = histogram.histogramToList();

        for (int intervalId = 0; intervalId < TopologyConfig.NUMBER_OF_INTERVALS; ++intervalId) {
            if (ballToBinMapping.get(intervalId) != null && ballToBinMapping.get(intervalId) != partitionId) {
                wordLoads.add(tmpWorkload);
                tmpWorkload = 0;
                partitionId = ballToBinMapping.get(intervalId);
            }

            tmpWorkload += workLoads.get(intervalId);
        }

        wordLoads.add(tmpWorkload);

        return wordLoads;
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
}
