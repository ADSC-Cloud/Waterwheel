package indexingTopology.util;

import java.io.Serializable;

/**
 * Created by acelzj on 11/28/16.
 */
public class FileScanMetrics implements Serializable {

    private TimeMetrics timeMetrics = new TimeMetrics();

    public String debugInfo;

    public FileScanMetrics() {

    }

    public void startEvent(String event) {
        timeMetrics.startEvent(event);
    }


    public Long endEvent(String event) {
        return timeMetrics.endEvent(event);
    }

    @Override
    public String toString() {
        return "metrics: " + timeMetrics.toString();
    }
}
