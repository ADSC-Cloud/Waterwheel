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

    public List<Integer> getRepartitionPlan() {
        Integer distance = (int) (workload / nbins);

        Object[] balls = ballToBinMapping.keySet().toArray();

        Arrays.sort(balls);

        Long totalWeight = 0L;

        List<Integer> binToBallMapping = new ArrayList<>();

        Integer bin = 0;

        System.out.println("ball to weight " + ballToWeight);

        for (Object ball : balls) {
            Long weight = ballToWeight.get(ball);
            totalWeight += weight;
            if (totalWeight >= distance) {
                binToBallMapping.add((Integer) ball);
                ++bin;
                totalWeight = 0L;
            }
        }
        binToBallMapping.add((Integer) balls[balls.length - 1]);

        return binToBallMapping;
    }

}
