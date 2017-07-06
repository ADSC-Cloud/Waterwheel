package indexingTopology.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Robert on 7/5/17.
 */
public class TimeMetrics implements Serializable {
    Map<String, Long> metrics = new HashMap<>();

    public void startEvent(String event) {
        metrics.put(event, System.currentTimeMillis());
    }

    public Long endEvent(String event) {
        if (!metrics.containsKey(event))
            return null;
        long duration = System.currentTimeMillis() - metrics.get(event);
        metrics.put(event, duration);
        return duration;
    }

    public String toString() {
        String string = "";
        for(String event: metrics.keySet()) {
            string += String.format("%s: %d ms. ", event, metrics.get(event));
        }
        return string;
    }
}
