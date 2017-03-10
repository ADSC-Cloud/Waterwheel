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
public class Aggregator<Key extends Number & Comparable<Key>> implements Serializable{

    private Map<Key, Object[]> aggregationResults = new HashMap<>();
    private AggregateField[] aggregateFields;
    final private int[] aggregateColumnIndexes;
    final private int groupByIndex;
    final DataSchema inputSchema;

    public Aggregator(DataSchema inputSchema, String groupByField, AggregateField... fields) {
        this.aggregateFields = fields;
        this.aggregateColumnIndexes = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            aggregateColumnIndexes[i] = inputSchema.getFieldIndex(fields[i].fieldName);
        }
        this.groupByIndex = inputSchema.getFieldIndex(groupByField);
        this.inputSchema = inputSchema;
    }

    public void aggregate(DataTuple dataTuple) {
        //TODO: performance optimization by computing the group-by column index before aggregate.
        Key group = (Key) dataTuple.get(groupByIndex);
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
                aggregationValues[i] = aggregateFields[i].function.aggregateFunction(dataTuple.get(aggregateColumnIndexes[i]), aggregationValues[i]);
            }
            return aggregationValues;
        });

//        aggregationResults.compute(group, (k, v) -> count.aggregateFunction(group, v));
//        aggregationResults.computeIfPresent(group, (k, v) -> aggregationResults.put(k, v + 1));
//        aggregationResults.put(group, count + 1);
    }

    public DataSchema getOutputDataSchema() {
        DataSchema dataSchema = new DataSchema();
        dataSchema.addField(inputSchema.getDataType(groupByIndex), inputSchema.getFieldName(groupByIndex));
        for (AggregateField aggregateField: aggregateFields) {
            if (aggregateField.function instanceof Count) {
                dataSchema.addDoubleField(String.format("count(%s)", aggregateField.fieldName));
            } else if (aggregateField.function instanceof Sum) {
                dataSchema.addDoubleField(String.format("sum(%s)", aggregateField.fieldName));
            } else if (aggregateField.function instanceof Min) {
                dataSchema.addField(inputSchema.getDataType(aggregateField.fieldName), String.format("min(%s)",
                        aggregateField.fieldName));
            } else if (aggregateField.function instanceof Max) {
                dataSchema.addField(inputSchema.getDataType(aggregateField.fieldName), String.format("max(%s)",
                        aggregateField.fieldName));
            }
        }
        return dataSchema;
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
