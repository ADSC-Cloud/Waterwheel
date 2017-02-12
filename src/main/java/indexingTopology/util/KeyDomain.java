package indexingTopology.util;

/**
 * Created by acelzj on 12/2/17.
 */
public class KeyDomain <TKey extends Comparable<TKey>> {
    TKey lowerBound;
    TKey upperBound;

    public KeyDomain(TKey lowerBound, TKey upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public TKey getLowerBound() {
        return lowerBound;
    }

    public TKey getUpperBound() {
        return upperBound;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof KeyDomain) {
            KeyDomain keyDomain = (KeyDomain) obj;
            return lowerBound.compareTo((TKey) keyDomain.getLowerBound()) == 0 &&
                    upperBound.compareTo((TKey) keyDomain.getUpperBound()) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = result + lowerBound.hashCode();
        result = result + upperBound.hashCode();
        return result;
    }
}
