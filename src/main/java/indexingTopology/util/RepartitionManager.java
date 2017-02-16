package indexingTopology.util;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class RepartitionManager {

    private Map<Integer, Integer> ballToBinMapping = new HashMap<>();

    private Long workload;

    private Map<Integer, Long> ballToWeight;

    private int nbins;

    public RepartitionManager(int numberOfBins, Map<Integer, Integer> ballToBinMapping,
                              Map<Integer, Long> ballToWeight, Long workload) {
        this.workload = workload;
        this.ballToBinMapping.putAll(ballToBinMapping);
        this.ballToWeight = ballToWeight;
        nbins = numberOfBins;
    }

    public Map<Integer, Integer> getRepartitionPlan() {
        Integer distance = (int) (workload / nbins);

        Object[] balls = ballToBinMapping.keySet().toArray();

        System.out.println(ballToBinMapping);

//        System.out.println(ballToWeight);

        long[] loadsBeforeRepartition = new long[nbins];

        for (Object ball : balls) {
            int bin = ballToBinMapping.get(ball);
            loadsBeforeRepartition[bin % nbins] += ballToWeight.get(ball);
        }

        for (int i = 0; i < loadsBeforeRepartition.length; ++i) {
            System.out.println("bin " + i + " : " + loadsBeforeRepartition[i]);
        }

        Map<Integer, Integer> newBallToBinMapping = new HashMap<>();

        Arrays.sort(balls);

        Long totalWeight = 0L;

        int bin = 0;

        for (Object ball : balls) {
            Long weight = ballToWeight.get(ball);
            totalWeight += weight;
            newBallToBinMapping.put((Integer) ball, bin);
            if (totalWeight >= distance) {
                ++bin;
                bin = Math.min(bin, nbins - 1);
                totalWeight = 0L;
            }
        }

        long[] loads = new long[nbins];

//        System.out.println(newBallToBinMapping);

        for (Object ball : balls) {
            bin = newBallToBinMapping.get(ball);
            loads[bin % nbins] += ballToWeight.get(ball);
        }

        System.out.println("Repartition has been finished!");
        for (int i = 0; i < loads.length; ++i) {
            System.out.println("bin " + i + " : " + loads[i]);
        }

        return newBallToBinMapping;
    }

}
