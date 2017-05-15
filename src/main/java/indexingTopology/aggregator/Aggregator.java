package indexingTopology.aggregator;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.PartialQueryResult;
import sun.security.provider.Sun;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by robert on 10/3/17.
 */
public class Aggregator<Key extends Number & Comparable<Key>> implements Serializable{

    // this is the computation state
    static public class IntermediateResult<Key extends Number & Comparable<Key>> {

        transient Map<Key, Object[]> aggregationResults = new HashMap<>();

        public String toString() {
            String str = "";
            for (Key key: aggregationResults.keySet()) {
                str += String.format("%d: ", key);
                for (Object object: aggregationResults.get(key)) {
                    str += String.format("%s\t", object);
                }
                str +="\n";
            }
            return str;
        }
    }

    public IntermediateResult<Key> createIntermediateResult() {
        return new IntermediateResult<>();
    }

    // the following are the data structures representing the aggregation logic.
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
            if (fields[i].function instanceof Count) {
                aggregateColumnIndexes[i] = 0;
            } else {
                aggregateColumnIndexes[i] = inputSchema.getFieldIndex(fields[i].fieldName);
            }
        }
        this.groupByIndex = inputSchema.getFieldIndex(groupByField);
        this.inputSchema = inputSchema;
        this.isGlobal = isGlobal;
    }

    public void aggregate(DataTuple dataTuple, IntermediateResult intermediateResult) {
        //TODO: performance optimization by computing the group-by column index before aggregate.



        Key group = (Key) dataTuple.get(groupByIndex);
        intermediateResult.aggregationResults.computeIfAbsent(group, p -> {
            Object[] aggregationValues = new Object[aggregateFields.length];
            for (int i = 0; i < aggregateFields.length; i++) {
                aggregationValues[i] = aggregateFields[i].function.init();
            }
            return aggregationValues;
        });

        intermediateResult.aggregationResults.compute(group, (k, v) -> {
            Object[] aggregationValues = (Object[]) v;
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
                dataSchema.addLongField(fieldName);
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



    public void aggregate(List<DataTuple> dataTupleList, IntermediateResult intermediateResult) {
        if (intermediateResult.aggregationResults == null) {
            intermediateResult.aggregationResults = new HashMap<>();
        }
        for (DataTuple dataTuple: dataTupleList) {
            aggregate(dataTuple, intermediateResult);
        }
    }

    public PartialQueryResult getResults(IntermediateResult<Key> intermediateResult) {
        PartialQueryResult partialQueryResult = new PartialQueryResult(Integer.MAX_VALUE);
        // aggregationResults may be null if no valid tuples are found before aggregation
//        if (aggregationResults != null) {
            for (Key group : intermediateResult.aggregationResults.keySet()) {
                final DataTuple dataTuple = new DataTuple();
                dataTuple.add(group);
                Object[] aggregationValues = intermediateResult.aggregationResults.get(group);
                for (Object object : aggregationValues) {
                    dataTuple.add(object);
                }
                partialQueryResult.add(dataTuple);
            }
//        }
        return partialQueryResult;
    }

    public Aggregator<Key> generateGlobalAggregator() {
        DataSchema globalInputSchema = getOutputDataSchema();
        AggregateField[] newAggregateFields = new AggregateField[aggregateFields.length];
        for (int i = 0; i < aggregateFields.length; i++) {
            if (aggregateFields[i].function instanceof Count)
                newAggregateFields[i] = new AggregateField(new Sum(), aggregateFields[i].aggregateFieldName());
            else
                newAggregateFields[i] = new AggregateField(aggregateFields[i].function, aggregateFields[i].aggregateFieldName());
        }
        return new Aggregator<>(globalInputSchema, inputSchema.getFieldName(groupByIndex), true, newAggregateFields);
    }

}
