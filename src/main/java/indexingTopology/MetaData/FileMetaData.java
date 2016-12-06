package indexingTopology.MetaData;

/**
 * Created by acelzj on 11/24/16.
 */
public class FileMetaData {

    String filename;
    long startTime;
    long endTime;
    double keyRangeLowerBound;
    double keyRangeUpperBound;

    public FileMetaData(String filename,  double keyRangeLowerBound, double keyRangeUpperBound,
                        long startTime, long endTime) {
        this.filename = filename;
        this.startTime = startTime;
        this.endTime = endTime;
        this.keyRangeUpperBound = keyRangeUpperBound;
        this.keyRangeLowerBound = keyRangeLowerBound;
    }
}
