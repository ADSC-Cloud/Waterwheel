package indexingTopology.util;

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

    public BackPressure() {
        this(5000);
    }

    public BackPressure(int emitNumber) {
        currentCount = new AtomicLong(0);
        this.emitNumber = emitNumber;
        pendingIds = new ArrayList<>();
    }

    public void ack(Long tupleId) {
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
}
