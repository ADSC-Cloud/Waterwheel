package indexingTopology.util;

/**
 * Created by acelzj on 13/1/17.
 */
public class Domain <TKey extends Comparable<TKey>>{

    private long startTimestamp;

    private long endTimestamp;

    private TKey lowerBound;

    private TKey upperBound;

    public Domain(long startTimestamp, long endTimestamp, TKey lowerBound, TKey upperBound) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof Domain) {
            Domain domain = (Domain) obj;
            return startTimestamp == domain.getStartTimestamp() &&
                    endTimestamp == domain.getEndTimestamp() &&
                    lowerBound.compareTo((TKey) domain.getLowerBound()) == 0 &&
                    upperBound.compareTo((TKey) domain.getUpperBound()) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result + (int)(startTimestamp ^ (startTimestamp >>> 32));
        result = result + (int)(endTimestamp ^ (endTimestamp >>> 32));
        result = result + lowerBound.hashCode();
        result = result + upperBound.hashCode();
        return result;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public TKey getLowerBound() {
        return lowerBound;
    }

    public TKey getUpperBound() {
        return upperBound;
    }
}
