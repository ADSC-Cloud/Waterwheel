package indexingTopology.util;

import indexingTopology.Config.Config;

import java.util.*;

/**
 * Created by acelzj on 12/12/16.
 */
public class Histogram {

    private Map<Integer, Long> histogram;

    private Double lowerBound;

    private Double upperBound;

    private int numberOfIntervals;

    public Histogram(Double lowerBound, Double upperBound, int numberOfIntervals) {
        histogram = new HashMap<>();
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.numberOfIntervals = numberOfIntervals;
    }

    public void record(int intervalId) {
        Long frequency = histogram.get(intervalId);
        if (frequency == null) {
            histogram.put(intervalId, 0L);
        } else {
            histogram.put(intervalId, frequency + 1L);
        }
    }

    public Map<Integer, Long> getHistogram() {
        return histogram;
    }

    public List<Long> histogramToList() {
        List<Long> ret = new ArrayList<>();
        setDefaultValueForAbsentKey(Config.NUMBER_OF_INTERVALS);
        Object[] keys = histogram.keySet().toArray();
        Arrays.sort(keys);
        for (Object key : keys) {
            ret.add(histogram.get(key));
        }
        return ret;
    }

    public void setDefaultValueForAbsentKey(int numberOfKeys) {
        for(int i=0; i< numberOfKeys; i++ ) {
            if(!histogram.containsKey(i)) {
                histogram.put(i, 0L );
            }
        }
    }

}
