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

    private List<Integer> bins;

    public RepartitionManager(int numberOfBins, Map<Integer, Integer> ballToBinMapping,
                              Map<Integer, Long> ballToWeight, Long workload, List<Integer> bins) {
        this.workload = workload;
        this.ballToBinMapping.putAll(ballToBinMapping);
        this.ballToWeight = ballToWeight;
        nbins = numberOfBins;
        this.bins = bins;
    }

    public Map<Integer, Integer> getRepartitionPlan() {
        Integer distance = (int) (workload / nbins);

        Object[] balls = ballToBinMapping.keySet().toArray();

        Map<Integer, Integer> newBallToBinMapping = new HashMap<>();

        Arrays.sort(balls);

        Long totalWeight = 0L;

//        System.out.println("ball to weight " + ballToWeight);

//        System.out.println("*****bin*****");

//        for (int i = 0; i < bins.size(); ++i) {
//            System.out.println("bin " + i + " " + bins.get(i));
//        }

        int bin = 0;

        for (Object ball : balls) {
            Long weight = ballToWeight.get(ball);
            totalWeight += weight;
            newBallToBinMapping.put((Integer) ball, bins.get(bin));
            if (totalWeight >= distance) {
                ++bin;
                totalWeight = 0L;
            }
        }

        long[] loads = new long[nbins];

        System.out.println(newBallToBinMapping);

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
