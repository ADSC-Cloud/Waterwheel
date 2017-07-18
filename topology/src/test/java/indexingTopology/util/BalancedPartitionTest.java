package indexingTopology.util;

import indexingTopology.util.partition.BalancedPartition;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created by acelzj on 12/17/16.
 */
public class BalancedPartitionTest extends TestCase{

    @Test
    public void testBoundary() {
        BalancedPartition<Integer> partition = new BalancedPartition<>(10, 16, 0, 100);
        assertEquals(0, partition.getIntervalId(-100));
        assertEquals(15, partition.getIntervalId(2330));
    }


    @Test
    public void testOneToOneMapping() {
        BalancedPartition<Integer> partition = new BalancedPartition<>(100, 100, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(i, partition.getPartitionId(i));
        }
    }


    @Test
    public void testTenToOneMapping() {
        BalancedPartition<Integer> partition = new BalancedPartition<>(10, 100, 0, 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(i / 10, partition.getPartitionId(i));
        }
    }
}