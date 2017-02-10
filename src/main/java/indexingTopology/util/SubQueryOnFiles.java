package indexingTopology.util;

/**
 * Created by acelzj on 10/2/17.
 */
public class SubQueryOnFiles extends SubQuery {

    String fileName;
    public SubQueryOnFiles(long queryId, Number leftKey, Number rightKey, String fileName, Long startTimestamp, Long endTimestamp) {
        super(queryId, leftKey, rightKey, startTimestamp, endTimestamp);
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}
