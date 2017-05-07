package indexingTopology.util.texi;

/**
 * Created by Robert on 12/11/16.
 */
public class Interval implements Comparable<Interval>{
    public int low;
    public int high;
    public Interval(int low, int high) {
        if(low > high)
            throw new IllegalArgumentException("Low should be no larger than high!");
        this.low = low;
        this.high = high;
    }

    public void merge(Interval interval) {
        low = Math.min(low, interval.low);
        high = Math.max(high, interval.high);
    }

    @Override
    public int compareTo(Interval o) {
        return Integer.compare(low, o.low);
    }
}
