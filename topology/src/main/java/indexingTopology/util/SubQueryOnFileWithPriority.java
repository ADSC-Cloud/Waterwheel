package indexingTopology.util;

import indexingTopology.common.aggregator.Aggregator;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.common.logics.DataTupleSorter;

import java.util.Comparator;

/**
 * Created by acelzj on 10/2/17.
 */
public class SubQueryOnFileWithPriority<T extends Number> extends SubQueryOnFile<T> implements Comparable{

    int priority;
    public SubQueryOnFileWithPriority(SubQueryOnFile<T> subQueryOnFile, int priority) {
        super(subQueryOnFile.queryId, subQueryOnFile.leftKey, subQueryOnFile.rightKey, subQueryOnFile.fileName,
                subQueryOnFile.startTimestamp, subQueryOnFile.endTimestamp, subQueryOnFile.predicate,
                subQueryOnFile.aggregator, subQueryOnFile.sorter);
        this.priority = priority;
    }


    @Override
    public int compareTo(Object o) {
        return Integer.compare(priority, ((SubQueryOnFileWithPriority)o).priority);
    }
}
