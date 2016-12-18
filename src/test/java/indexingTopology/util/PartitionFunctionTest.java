package indexingTopology.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 12/16/16.
 */
public class PartitionFunctionTest {
    @Test
    public void getIntervalIdNegativeLowerBound() throws Exception {
        PartitionFunction partitionFunction = new PartitionFunction(-500.0, 500.0);
        assertEquals(0, partitionFunction.getIntervalId(Double.NEGATIVE_INFINITY));
        assertEquals(0, partitionFunction.getIntervalId(-400.0));
        assertEquals(1, partitionFunction.getIntervalId(-350.0));
        assertEquals(1, partitionFunction.getIntervalId(-300.0));
        assertEquals(2, partitionFunction.getIntervalId(-250.0));
        assertEquals(2, partitionFunction.getIntervalId(-200.0));
        assertEquals(3, partitionFunction.getIntervalId(-150.0));
        assertEquals(3, partitionFunction.getIntervalId(-100.0));
        assertEquals(4, partitionFunction.getIntervalId(-50.0));
        assertEquals(4, partitionFunction.getIntervalId(0.0));
        assertEquals(5, partitionFunction.getIntervalId(50.0));
        assertEquals(5, partitionFunction.getIntervalId(100.0));
        assertEquals(6, partitionFunction.getIntervalId(150.0));
        assertEquals(6, partitionFunction.getIntervalId(200.0));
        assertEquals(7, partitionFunction.getIntervalId(250.0));
        assertEquals(7, partitionFunction.getIntervalId(300.0));
        assertEquals(8, partitionFunction.getIntervalId(350.0));
        assertEquals(8, partitionFunction.getIntervalId(400.0));
        assertEquals(9, partitionFunction.getIntervalId(500.0));
        assertEquals(9, partitionFunction.getIntervalId(Double.POSITIVE_INFINITY));
    }

    @Test
    public void getIntervalIdNegativeBounds() {
        PartitionFunction partitionFunction = new PartitionFunction(-2000.0, -1000.0);
        assertEquals(0, partitionFunction.getIntervalId(Double.NEGATIVE_INFINITY));
        assertEquals(0, partitionFunction.getIntervalId(-1900.0));
        assertEquals(1, partitionFunction.getIntervalId(-1850.0));
        assertEquals(1, partitionFunction.getIntervalId(-1800.0));
        assertEquals(2, partitionFunction.getIntervalId(-1750.0));
        assertEquals(2, partitionFunction.getIntervalId(-1700.0));
        assertEquals(3, partitionFunction.getIntervalId(-1650.0));
        assertEquals(3, partitionFunction.getIntervalId(-1600.0));
        assertEquals(4, partitionFunction.getIntervalId(-1550.0));
        assertEquals(4, partitionFunction.getIntervalId(-1500.0));
        assertEquals(5, partitionFunction.getIntervalId(-1450.0));
        assertEquals(5, partitionFunction.getIntervalId(-1400.0));
        assertEquals(6, partitionFunction.getIntervalId(-1350.0));
        assertEquals(6, partitionFunction.getIntervalId(-1300.0));
        assertEquals(7, partitionFunction.getIntervalId(-1250.0));
        assertEquals(7, partitionFunction.getIntervalId(-1200.0));
        assertEquals(8, partitionFunction.getIntervalId(-1150.0));
        assertEquals(8, partitionFunction.getIntervalId(-1100.0));
        assertEquals(9, partitionFunction.getIntervalId(-1000.0));
        assertEquals(9, partitionFunction.getIntervalId(Double.POSITIVE_INFINITY));
    }

    @Test
    public void getIntervalIdPositiveBounds() {
        PartitionFunction partitionFunction = new PartitionFunction(0.0, 1000.0);
        assertEquals(0, partitionFunction.getIntervalId(Double.NEGATIVE_INFINITY));
        assertEquals(0, partitionFunction.getIntervalId(100.0));
        assertEquals(1, partitionFunction.getIntervalId(150.0));
        assertEquals(1, partitionFunction.getIntervalId(200.0));
        assertEquals(2, partitionFunction.getIntervalId(250.0));
        assertEquals(2, partitionFunction.getIntervalId(300.0));
        assertEquals(3, partitionFunction.getIntervalId(350.0));
        assertEquals(3, partitionFunction.getIntervalId(400.0));
        assertEquals(4, partitionFunction.getIntervalId(450.0));
        assertEquals(4, partitionFunction.getIntervalId(500.0));
        assertEquals(5, partitionFunction.getIntervalId(550.0));
        assertEquals(5, partitionFunction.getIntervalId(600.0));
        assertEquals(6, partitionFunction.getIntervalId(650.0));
        assertEquals(6, partitionFunction.getIntervalId(700.0));
        assertEquals(7, partitionFunction.getIntervalId(750.0));
        assertEquals(7, partitionFunction.getIntervalId(800.0));
        assertEquals(8, partitionFunction.getIntervalId(850.0));
        assertEquals(8, partitionFunction.getIntervalId(900.0));
        assertEquals(9, partitionFunction.getIntervalId(1000.0));
        assertEquals(9, partitionFunction.getIntervalId(Double.POSITIVE_INFINITY));
    }

}