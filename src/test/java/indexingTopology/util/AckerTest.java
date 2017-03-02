package indexingTopology.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * Created by acelzj on 21/2/17.
 */
public class AckerTest {
    @Test
    public void ackWithoutPendingIds() throws Exception {
        Acker acker = new Acker();
        acker.currentCount = new AtomicLong(5000);

        acker.ack(10000L);
        assertEquals(10000L, acker.currentCount.get());
        assertEquals(0, acker.pendingIds.size());

        acker.ack(20000L);
        assertEquals(10000L, acker.currentCount.get());
        assertEquals(1, acker.pendingIds.size());

        acker.ack(25000L);
        assertEquals(10000L, acker.currentCount.get());
        assertEquals(2, acker.pendingIds.size());

        acker.ack(15000L);
        assertEquals(25000L, acker.currentCount.get());
        assertEquals(0, acker.pendingIds.size());
    }

    @Test
    public void ackLeavingPendingIds() throws Exception {
        Acker acker = new Acker();
        acker.currentCount = new AtomicLong(5000);

        acker.ack(10000L);
        assertEquals(10000L, acker.currentCount.get());
        assertEquals(0, acker.pendingIds.size());

        acker.ack(25000L);
        assertEquals(10000L, acker.currentCount.get());
        assertEquals(1, acker.pendingIds.size());

        acker.ack(15000L);
        assertEquals(15000L, acker.currentCount.get());
        assertEquals(1, acker.pendingIds.size());

    }

}