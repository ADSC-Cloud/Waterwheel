package indexingTopology.util;

import indexingTopology.config.TopologyConfig;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by Robert on 3/3/17.
 */
public class BackPressureTest {

    TopologyConfig config = new TopologyConfig();
    @Test
    public void testBlock() {
        BackPressure backPressure = new BackPressure(1, 2, config);
        assertTrue(backPressure.tryAcquireNextTupleId() != null);
        assertTrue(backPressure.tryAcquireNextTupleId() != null);
        assertTrue(backPressure.tryAcquireNextTupleId() == null);
    }

    @Test
    public void testACK() {
        BackPressure backPressure = new BackPressure(1,2, config);
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
        BackPressure backPressure = new BackPressure(1, 4, config);
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
        BackPressure backPressure = new BackPressure(2, 4, config);
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

    @Test
    public void TestNoDeadlock() {
        final int step = 5000;
        final int maxPending = 100000;
        final int tuples = 10000000;
        BackPressure backPressure = new BackPressure(step, maxPending, config);
        final LinkedBlockingQueue<Long> queue = new LinkedBlockingQueue<>();
        Thread consumerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Long id = queue.take();
                        if (id % step == 0) {
                            backPressure.ack(id);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        consumerThread.start();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Long> result = executor.submit(()->{
            long count = tuples;
            while(count-- > 0) {
                try {
                    Long id = backPressure.acquireNextTupleId();
                    queue.put(id);
                } catch (InterruptedException e) {

                }
            }
            return 0L;
        });

        long status = -1;
        try {
            status = result.get(10000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {

        }
        assertEquals(0L, status);

    }

    @Test
    public void ZipKeyRangeTest() {
        ZipfDistribution zipfDistribution = new ZipfDistribution(10000, 0.5);
        long count = 100000;
        while (count-- > 0) {
            assertTrue(zipfDistribution.sample() != 0);
        }
    }
}
