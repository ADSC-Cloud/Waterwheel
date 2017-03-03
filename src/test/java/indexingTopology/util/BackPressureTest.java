package indexingTopology.util;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by Robert on 3/3/17.
 */
public class BackPressureTest {
    @Test
    public void testBlock() {
        BackPressure backPressure = new BackPressure(1,2);
        assertTrue(backPressure.tryAcquireNextTupleId() != null);
        assertTrue(backPressure.tryAcquireNextTupleId() != null);
        assertTrue(backPressure.tryAcquireNextTupleId() == null);
    }

    @Test
    public void testACK() {
        BackPressure backPressure = new BackPressure(1,2);
        assertTrue(backPressure.tryAcquireNextTupleId() != null);
        assertTrue(backPressure.tryAcquireNextTupleId() != null);

        backPressure.ack(1L);
        assertEquals(2L, (long)backPressure.tryAcquireNextTupleId());

        assertEquals(null, backPressure.tryAcquireNextTupleId());

        backPressure.ack(3L);
        assertEquals(null, backPressure.tryAcquireNextTupleId());
    }

    @Test
    public void TestOverACK() {
        BackPressure backPressure = new BackPressure(1, 4);
        assertEquals(0L, (long)backPressure.tryAcquireNextTupleId());
        assertEquals(1L, (long)backPressure.tryAcquireNextTupleId());
        assertEquals(2L, (long)backPressure.tryAcquireNextTupleId());
        assertEquals(3L, (long)backPressure.tryAcquireNextTupleId());
        assertEquals(null, backPressure.tryAcquireNextTupleId());

        backPressure.ack(1L);
        assertEquals(4L, (long)backPressure.tryAcquireNextTupleId());

        backPressure.ack(3L);
        assertEquals(null, backPressure.tryAcquireNextTupleId());

        backPressure.ack(2L);
        assertEquals(5L, (long)backPressure.tryAcquireNextTupleId());
        assertEquals(6L, (long)backPressure.tryAcquireNextTupleId());

        assertEquals(null, backPressure.tryAcquireNextTupleId());
    }

    @Test
    public void TestInvalidedACK() {
        BackPressure backPressure = new BackPressure(2, 4);
        assertEquals(0L, (long) backPressure.tryAcquireNextTupleId());
        assertEquals(1L, (long) backPressure.tryAcquireNextTupleId());
        assertEquals(2L, (long) backPressure.tryAcquireNextTupleId());
        assertEquals(3L, (long) backPressure.tryAcquireNextTupleId());
        assertEquals(null, backPressure.tryAcquireNextTupleId());

        backPressure.ack(1L);// a invalid ack

        assertEquals(null, backPressure.tryAcquireNextTupleId());

        backPressure.ack(50L); // a valid but precocious ack
        assertEquals(null, backPressure.tryAcquireNextTupleId());

        backPressure.ack(2L); // a valid and expected ack
        assertEquals(4L, (long) backPressure.tryAcquireNextTupleId());
    }
}
