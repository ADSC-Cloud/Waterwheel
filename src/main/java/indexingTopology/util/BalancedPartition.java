package indexingTopology.util;

import indexingTopology.Config.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 12/17/16.
 */
public class BalancedPartition {

    private List<Integer> indexTasks;

    private Double lowerBound;

    private Double upperBound;

    public BalancedPartition(List<Integer> tasks, Double lowerBound, Double upperBound) {
        indexTasks = tasks;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }


    public Map<Integer, Integer> getBalancedPartitionPlan() {
        int numberOfTasks = indexTasks.size();
        Double distance = (upperBound - lowerBound) / numberOfTasks;
        Double miniDistance = (upperBound - lowerBound) / Config.NUMBER_OF_INTERVALS;
        Double keyRangeUpperBound = lowerBound + distance;
        Double bound = lowerBound + miniDistance;
        Map<Integer, Integer> intervalToTaskMapping = new HashMap<>();
        int index = 0;
        for (int i = 0; i < Config.NUMBER_OF_INTERVALS; ++i) {
            intervalToTaskMapping.put(i, indexTasks.get(index));
            bound += miniDistance;
            if (bound > keyRangeUpperBound) {
                keyRangeUpperBound = keyRangeUpperBound + distance;
//                bound = 0D;
                ++index;
            }
        }
        System.out.println("balanced partition has been finished!");
        System.out.println(intervalToTaskMapping);
        return intervalToTaskMapping;
    }


    /*
    public Map<Integer, Integer> getBalancedPartitionPlan() {
        int numberOfTasks = indexTasks.size();
        int numberOfintervalsOfTask =  Config.NUMBER_OF_INTERVALS / numberOfTasks;
        int index = 0;
        Map<Integer, Integer> intervalToTaskMapping = new HashMap<>();

        int count = 0;

        for (int i = 0; i < Config.NUMBER_OF_INTERVALS; ++i) {
            intervalToTaskMapping.put(i, indexTasks.get(index));
            ++count;
            if (count > numberOfintervalsOfTask) {
                count = 0;
                ++index;
            }
        }
        System.out.println("balanced partition has been finished!");
        System.out.println(intervalToTaskMapping);
        return intervalToTaskMapping;
    }
    */
}
