package indexingTopology.common;

import indexingTopology.common.SubQuery;
import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;

/**
 * Created by acelzj on 10/2/17.
 */
public class SubQueryOnFile<T extends Number> extends SubQuery<T> {

    String fileName;
    public SubQueryOnFile(long queryId, T leftKey, T rightKey, String fileName, Long startTimestamp, Long endTimestamp,
                          DataTuplePredicate dataTuplePredicate, DataTuplePredicate dataTuplePostPredicate,Aggregator aggregator, DataTupleSorter sorter) {
        super(queryId, leftKey, rightKey, startTimestamp, endTimestamp, dataTuplePredicate, dataTuplePostPredicate, aggregator, sorter);
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}
