package indexingTopology.aggregator;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.PartialQueryResult;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by robert on 10/3/17.
 */
public class Aggregator<Key extends Number> implements Serializable{

    private DataSchema inputSchema;
    private String groupByField;
    private Map<Key, Object[]> aggregationResults = new HashMap<>();
    private AggregateField[] aggregateFields;


    public Aggregator(DataSchema inputSchema, String groupByField, AggregateField... fields) {
        this.inputSchema = inputSchema;
        this.groupByField = groupByField;
        this.aggregateFields = fields;

    }

    public void aggregate(DataTuple dataTuple) {
        //TODO: performance optimization by computing the group-by column index before aggregate.
        Key group = (Key)inputSchema.getValue(groupByField, dataTuple);
        aggregationResults.computeIfAbsent(group, p -> {
            Object[] aggregationValues = new Object[aggregateFields.length];
            for (int i = 0; i < aggregateFields.length; i++) {
                aggregationValues[i] = aggregateFields[i].function.init();
            }
            return aggregationValues;
        });

        aggregationResults.compute(group, (k, v) -> {
            Object[] aggregationValues = v;
            for (int i = 0; i < aggregateFields.length; i++) {
                aggregationValues[i] = aggregateFields[i].function.aggregateFunction(inputSchema.getValue(aggregateFields[i].fieldName, dataTuple), aggregationValues[i]);
            }
            return aggregationValues;
        });

//        aggregationResults.compute(group, (k, v) -> count.aggregateFunction(group, v));
//        aggregationResults.computeIfPresent(group, (k, v) -> aggregationResults.put(k, v + 1));
//        aggregationResults.put(group, count + 1);
    }

    public void aggregate(List<DataTuple> dataTupleList) {
        for (DataTuple dataTuple: dataTupleList) {
            aggregate(dataTuple);
        }
    }

    public PartialQueryResult getResults() {
        PartialQueryResult partialQueryResult = new PartialQueryResult(Integer.MAX_VALUE);
        for(Key group: aggregationResults.keySet()) {
            final DataTuple dataTuple = new DataTuple();
            dataTuple.add(group);
            Object[] aggregationValues = aggregationResults.get(group);
            for(Object object: aggregationValues) {
                dataTuple.add(object);
            }
            partialQueryResult.add(dataTuple);
        }
        return partialQueryResult;
    }

}
