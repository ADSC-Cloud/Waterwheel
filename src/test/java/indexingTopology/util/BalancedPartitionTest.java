package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import junit.framework.TestCase;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/17/16.
 */
public class BalancedPartitionTest extends TestCase{

    int numberOfInterval;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        numberOfInterval = TopologyConfig.NUMBER_OF_INTERVALS;
        TopologyConfig.NUMBER_OF_INTERVALS = 10;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        TopologyConfig.NUMBER_OF_INTERVALS = numberOfInterval;
    }

    @Test
    public void testGetIntervalIdNegativeLowerBound() throws Exception {


        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10
        BalancedPartition partition = new BalancedPartition(4, -500.0, 500.0);
        assertEquals(0, partition.getIntervalId(Double.NEGATIVE_INFINITY));
        assertEquals(0, partition.getIntervalId(-400.0));
        assertEquals(1, partition.getIntervalId(-350.0));
        assertEquals(1, partition.getIntervalId(-300.0));
        assertEquals(2, partition.getIntervalId(-250.0));
        assertEquals(2, partition.getIntervalId(-200.0));
        assertEquals(3, partition.getIntervalId(-150.0));
        assertEquals(3, partition.getIntervalId(-100.0));
        assertEquals(4, partition.getIntervalId(-50.0));
        assertEquals(4, partition.getIntervalId(0.0));
        assertEquals(5, partition.getIntervalId(50.0));
        assertEquals(5, partition.getIntervalId(100.0));
        assertEquals(6, partition.getIntervalId(150.0));
        assertEquals(6, partition.getIntervalId(200.0));
        assertEquals(7, partition.getIntervalId(250.0));
        assertEquals(7, partition.getIntervalId(300.0));
        assertEquals(8, partition.getIntervalId(350.0));
        assertEquals(8, partition.getIntervalId(400.0));
        assertEquals(9, partition.getIntervalId(500.0));
        assertEquals(9, partition.getIntervalId(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testGetIntervalIdNegativeBounds() {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10
        BalancedPartition partition = new BalancedPartition(4, -2000.0, -1000.0);
        assertEquals(0, partition.getIntervalId(Double.NEGATIVE_INFINITY));
        assertEquals(0, partition.getIntervalId(-1900.0));
        assertEquals(1, partition.getIntervalId(-1850.0));
        assertEquals(1, partition.getIntervalId(-1800.0));
        assertEquals(2, partition.getIntervalId(-1750.0));
        assertEquals(2, partition.getIntervalId(-1700.0));
        assertEquals(3, partition.getIntervalId(-1650.0));
        assertEquals(3, partition.getIntervalId(-1600.0));
        assertEquals(4, partition.getIntervalId(-1550.0));
        assertEquals(4, partition.getIntervalId(-1500.0));
        assertEquals(5, partition.getIntervalId(-1450.0));
        assertEquals(5, partition.getIntervalId(-1400.0));
        assertEquals(6, partition.getIntervalId(-1350.0));
        assertEquals(6, partition.getIntervalId(-1300.0));
        assertEquals(7, partition.getIntervalId(-1250.0));
        assertEquals(7, partition.getIntervalId(-1200.0));
        assertEquals(8, partition.getIntervalId(-1150.0));
        assertEquals(8, partition.getIntervalId(-1100.0));
        assertEquals(9, partition.getIntervalId(-1000.0));
        assertEquals(9, partition.getIntervalId(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testGetIntervalIdPositiveBounds() {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10
        BalancedPartition partition = new BalancedPartition(4, 0.0, 1000.0);
        assertEquals(0, partition.getIntervalId(Double.NEGATIVE_INFINITY));
        assertEquals(0, partition.getIntervalId(100.0));
        assertEquals(1, partition.getIntervalId(150.0));
        assertEquals(1, partition.getIntervalId(200.0));
        assertEquals(2, partition.getIntervalId(250.0));
        assertEquals(2, partition.getIntervalId(300.0));
        assertEquals(3, partition.getIntervalId(350.0));
        assertEquals(3, partition.getIntervalId(400.0));
        assertEquals(4, partition.getIntervalId(450.0));
        assertEquals(4, partition.getIntervalId(500.0));
        assertEquals(5, partition.getIntervalId(550.0));
        assertEquals(5, partition.getIntervalId(600.0));
        assertEquals(6, partition.getIntervalId(650.0));
        assertEquals(6, partition.getIntervalId(700.0));
        assertEquals(7, partition.getIntervalId(750.0));
        assertEquals(7, partition.getIntervalId(800.0));
        assertEquals(8, partition.getIntervalId(850.0));
        assertEquals(8, partition.getIntervalId(900.0));
        assertEquals(9, partition.getIntervalId(1000.0));
        assertEquals(9, partition.getIntervalId(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testGetBalancedPartitionPlanPositiveBoundsWithOneTask() throws Exception {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10

        BalancedPartition partition = new BalancedPartition(4, 0.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();
        assertEquals(new Integer(0), intervalToTaskMapping.get(0));
        assertEquals(new Integer(0), intervalToTaskMapping.get(1));
        assertEquals(new Integer(1), intervalToTaskMapping.get(2));
        assertEquals(new Integer(1), intervalToTaskMapping.get(3));
        assertEquals(new Integer(1), intervalToTaskMapping.get(4));
        assertEquals(new Integer(2), intervalToTaskMapping.get(5));
        assertEquals(new Integer(2), intervalToTaskMapping.get(6));
        assertEquals(new Integer(3), intervalToTaskMapping.get(7));
        assertEquals(new Integer(3), intervalToTaskMapping.get(8));
        assertEquals(new Integer(3), intervalToTaskMapping.get(9));

    }

    @Test
    public void testGetBalancedPartitionPlanPositiveBoundsWithMultipleTasks() throws Exception {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10

        BalancedPartition partition = new BalancedPartition(4, 0.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();

        assertEquals(new Integer(0), intervalToTaskMapping.get(0));
        assertEquals(new Integer(0), intervalToTaskMapping.get(1));
        assertEquals(new Integer(1), intervalToTaskMapping.get(2));
        assertEquals(new Integer(1), intervalToTaskMapping.get(3));
        assertEquals(new Integer(1), intervalToTaskMapping.get(4));
        assertEquals(new Integer(2), intervalToTaskMapping.get(5));
        assertEquals(new Integer(2), intervalToTaskMapping.get(6));
        assertEquals(new Integer(3), intervalToTaskMapping.get(7));
        assertEquals(new Integer(3), intervalToTaskMapping.get(8));
        assertEquals(new Integer(3), intervalToTaskMapping.get(9));
    }

    @Test
    public void testGetBalancedPartitionPlanNegativeLowerBoundWithMultipleTasks() throws Exception {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10

        BalancedPartition partition = new BalancedPartition(4, -500.0, 500.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();

        assertEquals(new Integer(0), intervalToTaskMapping.get(0));
        assertEquals(new Integer(0), intervalToTaskMapping.get(1));
        assertEquals(new Integer(1), intervalToTaskMapping.get(2));
        assertEquals(new Integer(1), intervalToTaskMapping.get(3));
        assertEquals(new Integer(1), intervalToTaskMapping.get(4));
        assertEquals(new Integer(2), intervalToTaskMapping.get(5));
        assertEquals(new Integer(2), intervalToTaskMapping.get(6));
        assertEquals(new Integer(3), intervalToTaskMapping.get(7));
        assertEquals(new Integer(3), intervalToTaskMapping.get(8));
        assertEquals(new Integer(3), intervalToTaskMapping.get(9));
    }

    @Test
    public void testGetBalancedPartitionPlanNegativeLowerBoundWithOneTask() throws Exception {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10

        BalancedPartition partition = new BalancedPartition(4, -500.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();

        assertEquals(new Integer(0), intervalToTaskMapping.get(0));
        assertEquals(new Integer(0), intervalToTaskMapping.get(1));
        assertEquals(new Integer(1), intervalToTaskMapping.get(2));
        assertEquals(new Integer(1), intervalToTaskMapping.get(3));
        assertEquals(new Integer(1), intervalToTaskMapping.get(4));
        assertEquals(new Integer(2), intervalToTaskMapping.get(5));
        assertEquals(new Integer(2), intervalToTaskMapping.get(6));
        assertEquals(new Integer(3), intervalToTaskMapping.get(7));
        assertEquals(new Integer(3), intervalToTaskMapping.get(8));
        assertEquals(new Integer(3), intervalToTaskMapping.get(9));
    }


    @Test
    public void testGetBalancedPartitionPlanNegativeBoundsWithMultipleTasks() throws Exception {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10

        BalancedPartition partition = new BalancedPartition(4, -2000.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();

        assertEquals(new Integer(0), intervalToTaskMapping.get(0));
        assertEquals(new Integer(0), intervalToTaskMapping.get(1));
        assertEquals(new Integer(1), intervalToTaskMapping.get(2));
        assertEquals(new Integer(1), intervalToTaskMapping.get(3));
        assertEquals(new Integer(1), intervalToTaskMapping.get(4));
        assertEquals(new Integer(2), intervalToTaskMapping.get(5));
        assertEquals(new Integer(2), intervalToTaskMapping.get(6));
        assertEquals(new Integer(3), intervalToTaskMapping.get(7));
        assertEquals(new Integer(3), intervalToTaskMapping.get(8));
        assertEquals(new Integer(3), intervalToTaskMapping.get(9));
    }


    @Test
    public void testGetBalancedPartitionPlanNegativeBoundsWithOneTask() throws Exception {

        //The test passes when the NUMBER_OF_INTERVALS in the TopologyConfig is set to 10

        BalancedPartition partition = new BalancedPartition(4, -2000.0, 1000.0);
        Map<Integer, Integer> intervalToTaskMapping = partition.getBalancedPartitionPlan();

        assertEquals(new Integer(0), intervalToTaskMapping.get(0));
        assertEquals(new Integer(0), intervalToTaskMapping.get(1));
        assertEquals(new Integer(1), intervalToTaskMapping.get(2));
        assertEquals(new Integer(1), intervalToTaskMapping.get(3));
        assertEquals(new Integer(1), intervalToTaskMapping.get(4));
        assertEquals(new Integer(2), intervalToTaskMapping.get(5));
        assertEquals(new Integer(2), intervalToTaskMapping.get(6));
        assertEquals(new Integer(3), intervalToTaskMapping.get(7));
        assertEquals(new Integer(3), intervalToTaskMapping.get(8));
        assertEquals(new Integer(3), intervalToTaskMapping.get(9));
    }

}