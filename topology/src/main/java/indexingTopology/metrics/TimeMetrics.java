package indexingTopology.metrics;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Robert on 7/5/17.
 */
public class TimeMetrics implements Serializable {
    public Map<String, Double> metrics = new HashMap<>();

    public Double getEventTime(String event) {
        return metrics.get(event);
    }

    public void startEvent(String event) {
        metrics.put(event, (double)System.currentTimeMillis());
    }

    public Double endEvent(String event) {
        if (!metrics.containsKey(event))
            return null;
        Double duration = (double)System.currentTimeMillis() - metrics.get(event);
        metrics.put(event, duration);
        return duration;
    }

    public String toString() {
        String string = "";
        for(String event: metrics.keySet()) {
            string += String.format("%s: %.2f ms, ", event, metrics.get(event));
        }
        return string.substring(0, string.length() - 2);
    }

    static TimeMetrics aggregate(List<TimeMetrics> collection) {
        TimeMetrics result = new TimeMetrics();
        collection.forEach(t -> t.metrics.forEach(
                (e, time) -> result.metrics.put(e,
                        result.metrics.computeIfAbsent(e, x -> 0.0) + time)
        ));
        return result;
    }

    public static void main(String[] args) throws InterruptedException {
        TimeMetrics metrics = new TimeMetrics();
        metrics.metrics.put("event 1", 10.0);
        metrics.metrics.put("event 2", 8.0);
        metrics.metrics.put("event 3", 1.0);


        TimeMetrics metrics1 = new TimeMetrics();
        metrics1.metrics.put("event 1", 3.0);
        metrics1.metrics.put("event 2", 1.0);
        metrics1.metrics.put("event 4", 2.0);

        List<TimeMetrics> list = new ArrayList<>();
        list.add(metrics);
        list.add(metrics1);

        System.out.println(TimeMetrics.aggregate(list));

    }
}
