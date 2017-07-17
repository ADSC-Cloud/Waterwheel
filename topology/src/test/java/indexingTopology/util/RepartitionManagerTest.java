package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/22/16.
 */
public class RepartitionManagerTest {

    private TopologyConfig config = new TopologyConfig();

    @Test
    public void getSkewnessFactor() throws Exception {
        Map<Integer, Integer> ballToBinMapping = new HashMap<>();
        ballToBinMapping.put(0, 0);
        ballToBinMapping.put(1, 0);
        ballToBinMapping.put(2, 0);
        ballToBinMapping.put(3, 1);
        ballToBinMapping.put(4, 1);
        ballToBinMapping.put(5, 2);
        ballToBinMapping.put(6, 2);
        ballToBinMapping.put(7, 2);
        ballToBinMapping.put(8, 3);
        ballToBinMapping.put(9, 3);


        Map<Integer, Long> ballToWeightMapping = new HashMap<>();
        ballToWeightMapping.put(0, 300L);
        ballToWeightMapping.put(1, 300L);
        ballToWeightMapping.put(2, 500L);
        ballToWeightMapping.put(3, 50L);
        ballToWeightMapping.put(4, 50L);
        ballToWeightMapping.put(5, 200L);
        ballToWeightMapping.put(6, 200L);
        ballToWeightMapping.put(7, 100L);
        ballToWeightMapping.put(8, 200L);
        ballToWeightMapping.put(9, 100L);

        Histogram histogram = new Histogram(ballToWeightMapping, config.NUMBER_OF_INTERVALS);

        RepartitionManager manager = new RepartitionManager(4, config.NUMBER_OF_INTERVALS, ballToBinMapping, histogram);

        double skewnessFactor = 1.2;

        assertEquals(skewnessFactor, manager.getSkewnessFactor(), 0.00001);
    }

    @Test
    public void getWorkLoads() throws Exception {
        Map<Integer, Integer> ballToBinMapping = new HashMap<>();
        ballToBinMapping.put(0, 0);
        ballToBinMapping.put(1, 0);
        ballToBinMapping.put(2, 0);
        ballToBinMapping.put(3, 1);
        ballToBinMapping.put(4, 1);
        ballToBinMapping.put(5, 2);
        ballToBinMapping.put(6, 2);
        ballToBinMapping.put(7, 2);
        ballToBinMapping.put(8, 3);
        ballToBinMapping.put(9, 3);


        Map<Integer, Long> ballToWeightMapping = new HashMap<>();
        ballToWeightMapping.put(0, 300L);
        ballToWeightMapping.put(1, 300L);
        ballToWeightMapping.put(2, 500L);
        ballToWeightMapping.put(3, 50L);
        ballToWeightMapping.put(4, 50L);
        ballToWeightMapping.put(5, 200L);
        ballToWeightMapping.put(6, 200L);
        ballToWeightMapping.put(7, 100L);
        ballToWeightMapping.put(8, 200L);
        ballToWeightMapping.put(9, 100L);

        Histogram histogram = new Histogram(ballToWeightMapping, config.NUMBER_OF_INTERVALS);

        RepartitionManager manager = new RepartitionManager(4, config.NUMBER_OF_INTERVALS, ballToBinMapping, histogram);

        List<Long> workloads = new ArrayList<>();
        workloads.add(1100L);
        workloads.add(100L);
        workloads.add(500L);
        workloads.add(300L);

        assertEquals(workloads, manager.getWorkLoads());
    }

    @Test
    public void getTotalWorkLoad() throws Exception {
        Map<Integer, Integer> ballToBinMapping = new HashMap<>();
        ballToBinMapping.put(0, 0);
        ballToBinMapping.put(1, 0);
        ballToBinMapping.put(2, 0);
        ballToBinMapping.put(3, 1);
        ballToBinMapping.put(4, 1);
        ballToBinMapping.put(5, 2);
        ballToBinMapping.put(6, 2);
        ballToBinMapping.put(7, 2);
        ballToBinMapping.put(8, 3);
        ballToBinMapping.put(9, 3);


        Map<Integer, Long> ballToWeightMapping = new HashMap<>();
        ballToWeightMapping.put(0, 300L);
        ballToWeightMapping.put(1, 300L);
        ballToWeightMapping.put(2, 500L);
        ballToWeightMapping.put(3, 50L);
        ballToWeightMapping.put(4, 50L);
        ballToWeightMapping.put(5, 200L);
        ballToWeightMapping.put(6, 200L);
        ballToWeightMapping.put(7, 100L);
        ballToWeightMapping.put(8, 200L);
        ballToWeightMapping.put(9, 100L);

        Histogram histogram = new Histogram(ballToWeightMapping, config.NUMBER_OF_INTERVALS);

        RepartitionManager manager = new RepartitionManager(4, config.NUMBER_OF_INTERVALS, ballToBinMapping, histogram);

        Long totalWordload = 2000L;
        assertEquals(totalWordload, manager.getTotalWorkLoad(manager.getWorkLoads()));
    }

    @Test
    public void getRepartitionPlan() throws Exception {
        Map<Integer, Integer> ballToBinMapping = new HashMap<>();
        ballToBinMapping.put(0, 0);
        ballToBinMapping.put(1, 0);
        ballToBinMapping.put(2, 0);
        ballToBinMapping.put(3, 1);
        ballToBinMapping.put(4, 1);
        ballToBinMapping.put(5, 2);
        ballToBinMapping.put(6, 2);
        ballToBinMapping.put(7, 2);
        ballToBinMapping.put(8, 3);
        ballToBinMapping.put(9, 3);


        Map<Integer, Long> ballToWeightMapping = new HashMap<>();
        ballToWeightMapping.put(0, 300L);
        ballToWeightMapping.put(1, 300L);
        ballToWeightMapping.put(2, 500L);
        ballToWeightMapping.put(3, 50L);
        ballToWeightMapping.put(4, 50L);
        ballToWeightMapping.put(5, 200L);
        ballToWeightMapping.put(6, 200L);
        ballToWeightMapping.put(7, 100L);
        ballToWeightMapping.put(8, 200L);
        ballToWeightMapping.put(9, 100L);

        Histogram histogram = new Histogram(ballToWeightMapping, config.NUMBER_OF_INTERVALS);

        RepartitionManager manager = new RepartitionManager(4, config.NUMBER_OF_INTERVALS, ballToBinMapping, histogram);

        ballToBinMapping = manager.getRepartitionPlan();

        assertEquals((Integer) 0, ballToBinMapping.get(0));
        assertEquals((Integer) 0, ballToBinMapping.get(1));
        assertEquals((Integer) 1, ballToBinMapping.get(2));
        assertEquals((Integer) 2, ballToBinMapping.get(3));
        assertEquals((Integer) 2, ballToBinMapping.get(4));
        assertEquals((Integer) 2, ballToBinMapping.get(5));
        assertEquals((Integer) 2, ballToBinMapping.get(6));
        assertEquals((Integer) 3, ballToBinMapping.get(7));
        assertEquals((Integer) 3, ballToBinMapping.get(8));
        assertEquals((Integer) 3, ballToBinMapping.get(9));
    }

    @Test
    public void getRepartitionPlanTest() throws Exception {
        Map<Integer, Integer> ballToBinMapping = new HashMap<>();
        for (int i = 0; i < 23; ++i) {
            ballToBinMapping.put(i, 4);
        }


        Map<Integer, Long> ballToWeightMapping = new HashMap<>();
        for (int i = 0; i < 23; ++i) {
            ballToWeightMapping.put(i, 4L);
        }

        Histogram histogram = new Histogram(ballToWeightMapping, config.NUMBER_OF_INTERVALS);

        RepartitionManager manager = new RepartitionManager(10, config.NUMBER_OF_INTERVALS, ballToBinMapping, histogram);

        ballToBinMapping = manager.getRepartitionPlan();

        assertEquals((Integer) 0, ballToBinMapping.get(0));
        assertEquals((Integer) 0, ballToBinMapping.get(1));
        assertEquals((Integer) 0, ballToBinMapping.get(2));
        assertEquals((Integer) 1, ballToBinMapping.get(3));
        assertEquals((Integer) 1, ballToBinMapping.get(4));
        assertEquals((Integer) 2, ballToBinMapping.get(5));
        assertEquals((Integer) 2, ballToBinMapping.get(6));
        assertEquals((Integer) 3, ballToBinMapping.get(7));
        assertEquals((Integer) 3, ballToBinMapping.get(8));
        assertEquals((Integer) 3, ballToBinMapping.get(9));
        assertEquals((Integer) 4, ballToBinMapping.get(10));
        assertEquals((Integer) 4, ballToBinMapping.get(11));
        assertEquals((Integer) 5, ballToBinMapping.get(12));
        assertEquals((Integer) 5, ballToBinMapping.get(13));
        assertEquals((Integer) 6, ballToBinMapping.get(14));
        assertEquals((Integer) 6, ballToBinMapping.get(15));
        assertEquals((Integer) 6, ballToBinMapping.get(16));
        assertEquals((Integer) 7, ballToBinMapping.get(17));
        assertEquals((Integer) 7, ballToBinMapping.get(18));
        assertEquals((Integer) 8, ballToBinMapping.get(19));
        assertEquals((Integer) 8, ballToBinMapping.get(20));
        assertEquals((Integer) 9, ballToBinMapping.get(21));
        assertEquals((Integer) 9, ballToBinMapping.get(22));
    }

}