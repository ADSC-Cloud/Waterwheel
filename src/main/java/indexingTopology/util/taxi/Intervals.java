package indexingTopology.util.taxi;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Robert on 12/11/16.
 */
public class Intervals {
    public List<Interval> intervals = new ArrayList<Interval>();


    public void addInterval(Interval interval) {
        int l = 0, r = intervals.size() - 1;
        int target = -1;
        while(l <= r && target < 0) {
            int mid = (l + r) / 2;
            final Interval midInterval = intervals.get(mid);

            if(midInterval.low <= interval.low) {

                if (mid + 1 < intervals.size() && intervals.get(mid + 1).low > interval.low) {
                    target = mid;
                    l = mid;
                } else
                    l = mid + 1;
            } else if(midInterval.low > interval.low) {
                r = mid - 1;
            }
        }

        if (target < 0) {
            if (r < 0)
                intervals.add(0, interval);
            else if (intervals.size() - 1 < 0 || intervals.get(intervals.size() - 1).high < interval.low - 1) {
                //there is no existing interval, or the last interval has no overlap with the interval
                intervals.add(interval);
            }
            else {
                //there is overlap with the last interval
                final int lastIndex = intervals.size() - 1;
                intervals.get(lastIndex).merge(interval);
            }
        } else {
            if(target + 1 >= intervals.size() || interval.high < intervals.get(target + 1).low - 1) {
                // the target interval will not overlap with the next interval after merging the new interval.
                final int high = Math.max(intervals.get(target).high, interval.high);
                intervals.set(target, new Interval(intervals.get(target).low, high));
                intervals.get(target).merge(interval);
            } else {
                // the target interval will overlap with the next interval after merging the new interval.
                intervals.get(target).merge(interval);
                intervals.get(target).merge(intervals.get(target + 1));
                intervals.remove(target + 1);
            }
        }
    }

    public void addPoint(int point) {
        addInterval(new Interval(point, point));
    }

    public String toString() {
        String ret = "";
        for(Interval interval: intervals) {
            ret += String.format("[%d,%d]", interval.low, interval.high);
        }
        return ret;
    }

    public static void main(String[] args) {

    }
}
