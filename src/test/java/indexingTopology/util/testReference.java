package indexingTopology.util;

import indexingTopology.Config.TopologyConfig;

import java.util.*;

/**
 * Created by acelzj on 7/27/16.
 */
public class testReference {
    public static void main(String[] args) {
        List<Integer> targetTasks = new ArrayList<>();
        for (int i = 0; i < 4; ++i) {
            targetTasks.add(i);
        }
        testReference test = new testReference();
        Map<Integer, Integer> partition = test.getInitialPartition(0.0, 1000.0, targetTasks);
        System.out.println(partition);
    }

    public Map<Integer, Integer> getInitialPartition(Double lowerBound, Double upperBound, List<Integer> targetTasks) {
        int numberOfTasks = targetTasks.size();
        Integer distance = (int) ((upperBound - lowerBound) / numberOfTasks);
        Integer miniDistance = (int) ((upperBound - lowerBound) / TopologyConfig.NUMBER_OF_INTERVALS);
        Double keyRangeUpperBound = lowerBound + distance;
        Double bound = lowerBound + miniDistance;
        Map<Integer, Integer> IntervalIdToTaskId = new HashMap<>();
        Integer intervalId = 0;
        for (int i = 0; i < TopologyConfig.NUMBER_OF_INTERVALS; ++i) {
            IntervalIdToTaskId.put(i, intervalId);
            bound += miniDistance;
            if (bound > keyRangeUpperBound) {
                keyRangeUpperBound = keyRangeUpperBound + distance;
                ++intervalId;
            }
        }
        return IntervalIdToTaskId;
    }
}
