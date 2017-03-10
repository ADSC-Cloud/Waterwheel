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
    final boolean isGlobal;

    public Aggregator(DataSchema inputSchema, String groupByField, AggregateField... fields) {
        this(inputSchema, groupByField, false, fields);
    }

    public Aggregator(DataSchema inputSchema, String groupByField, boolean isGlobal, AggregateField... fields) {
        this.aggregateFields = fields;
        this.aggregateColumnIndexes = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            aggregateColumnIndexes[i] = inputSchema.getFieldIndex(fields[i].fieldName);
        }
        this.groupByIndex = inputSchema.getFieldIndex(groupByField);
        this.inputSchema = inputSchema;
        this.isGlobal = isGlobal;
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
            String fieldName;

            if (isGlobal)
                fieldName = aggregateField.fieldName;
            else
                fieldName = aggregateField.aggregateFieldName();

            if (aggregateField.function instanceof Count) {
                dataSchema.addDoubleField(fieldName);
            } else if (aggregateField.function instanceof Sum) {
                dataSchema.addDoubleField(fieldName);
            } else if (aggregateField.function instanceof Min) {
                dataSchema.addField(inputSchema.getDataType(aggregateField.fieldName), fieldName);
            } else if (aggregateField.function instanceof Max) {
                dataSchema.addField(inputSchema.getDataType(aggregateField.fieldName), fieldName);
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

    public Aggregator<Key> generateGlobalAggregator() {
        DataSchema globalInputSchema = getOutputDataSchema();
        AggregateField[] newAggregateFields = new AggregateField[aggregateFields.length];
        for (int i = 0; i < aggregateFields.length; i++) {
            newAggregateFields[i] = new AggregateField(aggregateFields[i].function, aggregateFields[i].aggregateFieldName());
        }
        return new Aggregator<Key>(globalInputSchema, inputSchema.getFieldName(groupByIndex), true, newAggregateFields);
    }

}
