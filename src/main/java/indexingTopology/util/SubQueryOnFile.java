package indexingTopology.util;

import indexingTopology.aggregator.Aggregator;

/**
 * Created by acelzj on 10/2/17.
 */
public class SubQueryOnFile extends SubQuery {

    String fileName;
    public SubQueryOnFile(long queryId, Number leftKey, Number rightKey, String fileName, Long startTimestamp, Long endTimestamp, DataTuplePredicate dataTuplePredicate, Aggregator aggregator) {
        super(queryId, leftKey, rightKey, startTimestamp, endTimestamp, dataTuplePredicate, aggregator);
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}
