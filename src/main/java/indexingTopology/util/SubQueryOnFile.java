package indexingTopology.util;

import indexingTopology.aggregator.Aggregator;

/**
 * Created by acelzj on 10/2/17.
 */
public class SubQueryOnFile<T extends Number> extends SubQuery<T> {

    String fileName;
    public SubQueryOnFile(long queryId, T leftKey, T rightKey, String fileName, Long startTimestamp, Long endTimestamp, DataTuplePredicate dataTuplePredicate, Aggregator aggregator) {
        super(queryId, leftKey, rightKey, startTimestamp, endTimestamp, dataTuplePredicate, aggregator);
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}
