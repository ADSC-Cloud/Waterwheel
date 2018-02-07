package indexingTopology.util;

import indexingTopology.config.TopologyConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by acelzj on 21/2/17.
 */
public class BackPressure {

    public AtomicLong currentCount;

    public int emitNumber;

    public List<Long> pendingIds;

    private AtomicLong tupleId;

    private long maxPending;

    private TopologyConfig config;

    public BackPressure(TopologyConfig config) {
        this(5000, config.MAX_PENDING, config);
    }

    public BackPressure(int emitNumber, long maxPending, TopologyConfig config) {
        currentCount = new AtomicLong(0);
        this.emitNumber = emitNumber;
        pendingIds = new ArrayList<>();
        tupleId = new AtomicLong(0);
        this.maxPending = maxPending;
        this.config = config;
    }

    public void ack(Long tupleId) {
        if (tupleId % emitNumber != 0) {
            return;
        }
        if (tupleId == currentCount.get() + emitNumber) {
//            currentCount = tupleId;
            currentCount.set(tupleId);
            if (pendingIds.size() != 0) {
//                currentCount = pendingIds.get(pendingIds.size() - 1);
//                currentCount.set(pendingIds.get(pendingIds.size() - 1));
//                pendingIds.clear();
                List<Long> idsToRemoved = new ArrayList<>();
                for (Long pendingId : pendingIds) {
//                    Long pendingId = pendingIds.get(i);
                    if (pendingId == currentCount.get() + emitNumber) {
                        currentCount.set(pendingId);
                        idsToRemoved.add(pendingId);
                    } else {
                        break;
                    }
                }

                for (Long pendingId : idsToRemoved) {
                    pendingIds.remove(pendingId);
                }
            }
        } else if (tupleId > currentCount.get()) {
            pendingIds.add(tupleId);
            Collections.sort(pendingIds);
        } else {
//            throw new RuntimeException("Tuple id can't be smaller than current count");
        }
    }

    public Long acquireNextTupleId() throws InterruptedException {
        int count = 0;
        while (tupleId.get() >= currentCount.get() + maxPending) {
//            try {
                Thread.sleep(1);
//                if(count++ % 100 == 0) {
//                    System.out.println(String.format("TupleId: %d, currentCount: %d, maxPending: %d", tupleId.get(),
//                            currentCount.get(), maxPending));
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
        final Long id = tupleId.getAndIncrement();
        return id;
    }

    public boolean noPendingTuple() {
        return tupleId.get() == currentCount.get();
    }

    public synchronized Long tryAcquireNextTupleId() {
        if (tupleId.get() >= currentCount.get() + maxPending)
            return null;
        else
            return tupleId.getAndIncrement();
    }

    public long getCurrentCount() {
        return currentCount.get();
    }

    public String toString() {
        return String.format("tupleId: %d, currentCount: %d, pending: %s", tupleId.get(), currentCount.get(), pendingIds.toString());
    }

}
