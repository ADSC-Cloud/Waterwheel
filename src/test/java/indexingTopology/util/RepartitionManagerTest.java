package indexingTopology.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/22/16.
 */
public class RepartitionManagerTest {

    @Test
    public void getRepartitionPlan() throws Exception {
        Map<Integer, Integer> ballToBinMapping = new HashMap<>();
        ballToBinMapping.put(1, 0);
        ballToBinMapping.put(2, 0);
        ballToBinMapping.put(3, 0);
        ballToBinMapping.put(4, 1);
        ballToBinMapping.put(5, 1);
        ballToBinMapping.put(6, 2);
        ballToBinMapping.put(7, 2);
        ballToBinMapping.put(8, 2);
        ballToBinMapping.put(9, 3);
        ballToBinMapping.put(10, 3);

        Map<Integer, Long> ballToWeightMapping = new HashMap<>();
        ballToWeightMapping.put(1, 300L);
        ballToWeightMapping.put(2, 300L);
        ballToWeightMapping.put(3, 500L);
        ballToWeightMapping.put(4, 50L);
        ballToWeightMapping.put(5, 50L);
        ballToWeightMapping.put(6, 200L);
        ballToWeightMapping.put(7, 200L);
        ballToWeightMapping.put(8, 100L);
        ballToWeightMapping.put(9, 200L);
        ballToWeightMapping.put(10, 100L);

        RepartitionManager manager = new RepartitionManager(4,ballToBinMapping,
                ballToWeightMapping, 2000L);

        ballToBinMapping = manager.getRepartitionPlan();

        assertEquals((Integer) 0, ballToBinMapping.get(1));
        assertEquals((Integer) 0, ballToBinMapping.get(2));
        assertEquals((Integer) 1, ballToBinMapping.get(3));
        assertEquals((Integer) 2, ballToBinMapping.get(4));
        assertEquals((Integer) 2, ballToBinMapping.get(5));
        assertEquals((Integer) 2, ballToBinMapping.get(6));
        assertEquals((Integer) 2, ballToBinMapping.get(7));
        assertEquals((Integer) 3, ballToBinMapping.get(8));
        assertEquals((Integer) 3, ballToBinMapping.get(9));
        assertEquals((Integer) 3, ballToBinMapping.get(10));
    }

}