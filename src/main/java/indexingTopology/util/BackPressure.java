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

    private Long tupleId;

    private long maxPending;

    public BackPressure() {
        this(5000, TopologyConfig.MAX_PENDING);
    }

    public BackPressure(int emitNumber, long maxPending) {
        currentCount = new AtomicLong(0);
        this.emitNumber = emitNumber;
        pendingIds = new ArrayList<>();
        tupleId = 0L;
        this.maxPending = maxPending;
    }

    public void ack(Long tupleId) {
        if (tupleId % emitNumber != 0)
            return;
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
//                    System.out.println(pendingIds);
                }
            }
        } else if (tupleId > currentCount.get()) {
            pendingIds.add(tupleId);
            Collections.sort(pendingIds);
        } else {
            new RuntimeException("Tuple id can't be smaller than current count");
        }
    }

    public Long acquireNextTupleId() throws InterruptedException {
        while (tupleId >= currentCount.get() + maxPending) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return tupleId++;
    }

    public Long tryAcquireNextTupleId() {
        if (tupleId >= currentCount.get() + maxPending)
            return null;
        else
            return tupleId++;
    }

}
