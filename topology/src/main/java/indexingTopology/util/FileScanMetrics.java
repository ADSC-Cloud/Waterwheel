package indexingTopology.util;

import java.io.Serializable;

/**
 * Created by acelzj on 11/28/16.
 */
public class FileScanMetrics implements Serializable {

    private TimeMetrics timeMetrics = new TimeMetrics();

    public Tags tags = new Tags();

    public String debugInfo;

    public FileScanMetrics() {

    }

    public void setTag(String key, String value) {
        tags.setTag(key, value);
    }

    public void startEvent(String event) {
        timeMetrics.startEvent(event);
    }


    public Double endEvent(String event) {
        return timeMetrics.endEvent(event);
    }

    @Override
    public String toString() {
        return "metrics: " + timeMetrics.toString();
    }

    public static void main(String[] args) throws InterruptedException {
        FileScanMetrics metrics = new FileScanMetrics();
        metrics.startEvent("a");
        Thread.sleep(10);
        metrics.endEvent("a");
        System.out.println(metrics);
    }
}
