package indexingTopology.util;

import java.io.Serializable;

/**
 * Created by acelzj on 12/2/17.
 */
public class TimeDomain implements Serializable {
    private Long startTimestamp;

    private Long endTimestamp;

    public TimeDomain(Long startTimestamp, Long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof TimeDomain) {
            TimeDomain timeDomain = (TimeDomain) obj;
            return timeDomain.getStartTimestamp() == this.startTimestamp &&
                    timeDomain.getEndTimestamp() == this.endTimestamp;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result + (int)(startTimestamp ^ (startTimestamp >>> 32));
        result = result + (int)(endTimestamp ^ (endTimestamp >>> 32));
        return result;
    }
}
