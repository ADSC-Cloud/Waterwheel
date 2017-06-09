package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 21/2/17.
 */
public class AckerTest {
    @Test
    public void ackWithoutPendingIds() throws Exception {
        BackPressure backPressure = new BackPressure(new TopologyConfig());
        backPressure.currentCount = new AtomicLong(5000);

        backPressure.ack(10000L);
        assertEquals(10000L, backPressure.currentCount.get());
        assertEquals(0, backPressure.pendingIds.size());

        backPressure.ack(20000L);
        assertEquals(10000L, backPressure.currentCount.get());
        assertEquals(1, backPressure.pendingIds.size());

        backPressure.ack(25000L);
        assertEquals(10000L, backPressure.currentCount.get());
        assertEquals(2, backPressure.pendingIds.size());

        backPressure.ack(15000L);
        assertEquals(25000L, backPressure.currentCount.get());
        assertEquals(0, backPressure.pendingIds.size());
    }

    @Test
    public void ackLeavingPendingIds() throws Exception {
        BackPressure backPressure = new BackPressure(new TopologyConfig());
        backPressure.currentCount = new AtomicLong(5000);

        backPressure.ack(10000L);
        assertEquals(10000L, backPressure.currentCount.get());
        assertEquals(0, backPressure.pendingIds.size());

        backPressure.ack(25000L);
        assertEquals(10000L, backPressure.currentCount.get());
        assertEquals(1, backPressure.pendingIds.size());

        backPressure.ack(15000L);
        assertEquals(15000L, backPressure.currentCount.get());
        assertEquals(1, backPressure.pendingIds.size());

    }

}