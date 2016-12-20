package indexingTopology.util;

import indexingTopology.Config.TopologyConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/17/16.
 */
public class BalancedPartitionTest {

    @Test
    public void getBalancedPartitionPlanPositiveBoundsWithOneTask() throws Exception {
        List<Integer> tasks = new ArrayList<>();
        Integer taskId = 4;
        tasks.add(taskId);

        BalancedPartition partition = new BalancedPartition(4, 0.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        for (int i = 0; i < TopologyConfig.NUMBER_OF_INTERVALS; ++i) {
            assertEquals(taskId, intervalToTaskMapping.get(i));
        }
    }

    @Test
    public void getBalancedPartitionPlanPositiveBoundsWithMultipleTasks() throws Exception {
        List<Integer> tasks = new ArrayList<>();
        for (Integer i = 4; i < 8; ++i) {
            tasks.add(i);
        }
        BalancedPartition partition = new BalancedPartition(4, 0.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        assertEquals(new Integer(4), intervalToTaskMapping.get(0));
        assertEquals(new Integer(4), intervalToTaskMapping.get(1));
        assertEquals(new Integer(5), intervalToTaskMapping.get(2));
        assertEquals(new Integer(5), intervalToTaskMapping.get(3));
        assertEquals(new Integer(5), intervalToTaskMapping.get(4));
        assertEquals(new Integer(6), intervalToTaskMapping.get(5));
        assertEquals(new Integer(6), intervalToTaskMapping.get(6));
        assertEquals(new Integer(7), intervalToTaskMapping.get(7));
        assertEquals(new Integer(7), intervalToTaskMapping.get(8));
        assertEquals(new Integer(7), intervalToTaskMapping.get(9));
    }

    @Test
    public void getBalancedPartitionPlanNegativeLowerBoundWithMultipleTasks() throws Exception {
        List<Integer> tasks = new ArrayList<>();
        for (Integer i = 4; i < 8; ++i) {
            tasks.add(i);
        }
        BalancedPartition partition = new BalancedPartition(4, -500.0, 500.0);

        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        assertEquals(new Integer(4), intervalToTaskMapping.get(0));
        assertEquals(new Integer(4), intervalToTaskMapping.get(1));
        assertEquals(new Integer(5), intervalToTaskMapping.get(2));
        assertEquals(new Integer(5), intervalToTaskMapping.get(3));
        assertEquals(new Integer(5), intervalToTaskMapping.get(4));
        assertEquals(new Integer(6), intervalToTaskMapping.get(5));
        assertEquals(new Integer(6), intervalToTaskMapping.get(6));
        assertEquals(new Integer(7), intervalToTaskMapping.get(7));
        assertEquals(new Integer(7), intervalToTaskMapping.get(8));
        assertEquals(new Integer(7), intervalToTaskMapping.get(9));
    }

    @Test
    public void getBalancedPartitionPlanNegativeLowerBoundWithOneTask() throws Exception {
        List<Integer> tasks = new ArrayList<>();
        Integer taskId = 4;
        tasks.add(taskId);
        BalancedPartition partition = new BalancedPartition(4, -500.0, 1000.0);

        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        for (int i = 0; i < TopologyConfig.NUMBER_OF_INTERVALS; ++i) {
            assertEquals(taskId, intervalToTaskMapping.get(i));
        }
    }


    @Test
    public void getBalancedPartitionPlanNegativeBoundsWithMultipleTasks() throws Exception {
        List<Integer> tasks = new ArrayList<>();
        for (Integer i = 4; i < 8; ++i) {
            tasks.add(i);
        }
        BalancedPartition partition = new BalancedPartition(4, -2000.0, 1000.0);

        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        assertEquals(new Integer(4), intervalToTaskMapping.get(0));
        assertEquals(new Integer(4), intervalToTaskMapping.get(1));
        assertEquals(new Integer(5), intervalToTaskMapping.get(2));
        assertEquals(new Integer(5), intervalToTaskMapping.get(3));
        assertEquals(new Integer(5), intervalToTaskMapping.get(4));
        assertEquals(new Integer(6), intervalToTaskMapping.get(5));
        assertEquals(new Integer(6), intervalToTaskMapping.get(6));
        assertEquals(new Integer(7), intervalToTaskMapping.get(7));
        assertEquals(new Integer(7), intervalToTaskMapping.get(8));
        assertEquals(new Integer(7), intervalToTaskMapping.get(9));
    }


    @Test
    public void getBalancedPartitionPlanNegativeBoundsWithOneTask() throws Exception {
        List<Integer> tasks = new ArrayList<>();
        Integer taskId = 4;
        tasks.add(taskId);

        BalancedPartition partition = new BalancedPartition(4, -2000.0, 1000.0);

        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        for (int i = 0; i < TopologyConfig.NUMBER_OF_INTERVALS; ++i) {
            assertEquals(taskId, intervalToTaskMapping.get(i));
        }
    }

}