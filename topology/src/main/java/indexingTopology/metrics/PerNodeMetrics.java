package indexingTopology.metrics;

import indexingTopology.common.Histogram;

import java.io.Serializable;

/**
 * Created by robert on 23/8/17.
 */
public class PerNodeMetrics implements Serializable {

    public PerNodeMetrics(Histogram histogram, double cpu, double totalDiskSpace, double freeDiskSpace) {
        this.histogram = histogram;
        this.CPULoad = cpu;
        this.totalDiskSpace = totalDiskSpace;
        this.freeDiskSpace = freeDiskSpace;
    }
    public Histogram histogram;
    public double CPULoad;
    public double totalDiskSpace;
    public double freeDiskSpace;
}
