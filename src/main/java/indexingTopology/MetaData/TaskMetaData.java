package indexingTopology.MetaData;

/**
 * Created by acelzj on 11/24/16.
 */
public class TaskMetaData {

    int taskId;
    long startTime;
    long endTime;
    double keyRangeLowerBound;
    double keyRangeUpperBound;

    public TaskMetaData(int taskId,  double keyRangeLowerBound, double keyRangeUpperBound, long startTime, long endTime) {
        this.taskId = taskId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.keyRangeUpperBound = keyRangeUpperBound;
        this.keyRangeLowerBound = keyRangeLowerBound;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setKeyRangeLowerBound(Double keyRangeLowerBound) {
        this.keyRangeLowerBound = keyRangeLowerBound;
    }

    public void setKeyRangeUpperBound(Double keyRangeUpperBound) {
        this.keyRangeUpperBound = keyRangeUpperBound;
    }
}
