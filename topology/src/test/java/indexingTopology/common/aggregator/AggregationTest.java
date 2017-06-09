package indexingTopology.common.aggregator;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.PartialQueryResult;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Test;

import java.util.Collections;

import static junit.framework.TestCase.*;
/**
 * Created by Robert on 3/10/17.
 */
public class AggregationTest {

    @Test
    public void testScalarAggregationEmptyInput() {
        DataSchema schema = new DataSchema();
        schema.addIntField("id");
        schema.addIntField("group");
        schema.addIntField("height");
        PartialQueryResult partialQueryResult = new PartialQueryResult();

        Aggregator<Integer> aggregator = new Aggregator<>(schema, null, new AggregateField[]{
                new AggregateField(new Count<>(), "group"),
                new AggregateField(new Sum<>(), "height"),
                new AggregateField(new Max<>(), "height"),
                new AggregateField(new Min<>(), "height")
        });

        Aggregator.IntermediateResult intermediateResult = aggregator.createIntermediateResult();
        aggregator.aggregate(partialQueryResult.dataTuples, intermediateResult);


        PartialQueryResult result = aggregator.getResults(intermediateResult);
        assertEquals(1, result.dataTuples.size());
        assertEquals(0L, result.dataTuples.get(0).get(0));
        assertEquals(null, result.dataTuples.get(0).get(1));
        assertEquals(null, result.dataTuples.get(0).get(2));
        assertEquals(null, result.dataTuples.get(0).get(3));
    }

    @Test
    public void testAggregation1() {
        DataSchema schema = new DataSchema();
        schema.addIntField("id");
        schema.addIntField("group");
        schema.addIntField("height");


        PartialQueryResult partialQueryResult = new PartialQueryResult();
        partialQueryResult.add(new DataTuple(1, 1, 180));
        partialQueryResult.add(new DataTuple(2, 2, 176));
        partialQueryResult.add(new DataTuple(3, 2, 172));
        partialQueryResult.add(new DataTuple(4, 3, 183));
        partialQueryResult.add(new DataTuple(5, 3, 167));

        Aggregator<Integer> aggregator = new Aggregator<>(schema, "group", new AggregateField[]{
                new AggregateField(new Count<>(), "group"),
                new AggregateField(new Sum<>(), "height"),
                new AggregateField(new Max<>(), "height"),
                new AggregateField(new Min<>(), "height")

        });
        Aggregator.IntermediateResult intermediateResult = aggregator.createIntermediateResult();
        aggregator.aggregate(partialQueryResult.dataTuples, intermediateResult);


        PartialQueryResult result = aggregator.getResults(intermediateResult);
        assertEquals(result.dataTuples.get(0), new DataTuple(1, 1L, 180.0, 180, 180));
        assertEquals(result.dataTuples.get(1), new DataTuple(2, 2L, 348.0, 176, 172));
        assertEquals(result.dataTuples.get(2), new DataTuple(3, 2L, 350.0, 183, 167));

    }

    @Test
    public void testAggregation2() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        PartialQueryResult partialQueryResult = new PartialQueryResult();
        partialQueryResult.add(new DataTuple(1, 1.0, 1L));
        partialQueryResult.add(new DataTuple(2, 2.0, 2L));
        partialQueryResult.add(new DataTuple(2, 1.0, 2L));
        partialQueryResult.add(new DataTuple(3, 3.0, 2L));
        partialQueryResult.add(new DataTuple(3, 1.0, 3L));

        Aggregator<Integer> aggregator = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });
        Aggregator.IntermediateResult intermediateResult = aggregator.createIntermediateResult();
        aggregator.aggregate(partialQueryResult.dataTuples, intermediateResult);


        PartialQueryResult result = aggregator.getResults(intermediateResult);
        assertEquals(result.dataTuples.get(0), new DataTuple(1, 1L, 1.0, 1.0, 1L, 1L, 1.0));
        assertEquals(result.dataTuples.get(1), new DataTuple(2, 2L, 3.0, 2.0, 2L, 2L, 4.0));
        assertEquals(result.dataTuples.get(2), new DataTuple(3, 2L, 4.0, 3.0, 3L, 2L, 5.0));

    }

    @Test
    public void testAggregationByDoubleFileld() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        PartialQueryResult partialQueryResult = new PartialQueryResult();
        partialQueryResult.add(new DataTuple(1.0, 1.0, 1L));
        partialQueryResult.add(new DataTuple(2.0, 2.0, 2L));
        partialQueryResult.add(new DataTuple(2.0, 1.0, 2L));
        partialQueryResult.add(new DataTuple(3.0, 3.0, 2L));
        partialQueryResult.add(new DataTuple(3.0, 1.0, 3L));

        Aggregator<Integer> aggregator = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });
        Aggregator.IntermediateResult intermediateResult = aggregator.createIntermediateResult();
        aggregator.aggregate(partialQueryResult.dataTuples, intermediateResult);


        PartialQueryResult result = aggregator.getResults(intermediateResult);
        Collections.sort(result.dataTuples, (DataTuple t1, DataTuple t2) -> ((Comparable)t1.get(0)).compareTo(t2.get(0)) );
        assertEquals(result.dataTuples.get(0), new DataTuple(1.0, 1L, 1.0, 1.0, 1L, 1L, 1.0));
        assertEquals(result.dataTuples.get(1), new DataTuple(2.0, 2L, 3.0, 2.0, 2L, 2L, 4.0));
        assertEquals(result.dataTuples.get(2), new DataTuple(3.0, 2L, 4.0, 3.0, 3L, 2L, 5.0));

    }

    @Test
    public void testOutputSchema() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        Aggregator<Integer> aggregator = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });

        DataSchema outputSchema = aggregator.getOutputDataSchema();
        assertEquals(Integer.class, outputSchema.getDataType(0).type);
        assertEquals(Long.class, outputSchema.getDataType(1).type);
        assertEquals(Double.class, outputSchema.getDataType(2).type);
        assertEquals(Double.class, outputSchema.getDataType(3).type);
        assertEquals(Long.class, outputSchema.getDataType(4).type);
        assertEquals(Long.class, outputSchema.getDataType(5).type);
        assertEquals(Double.class, outputSchema.getDataType(6).type);

    }

    @Test
    public void testGlobalAggregator() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        Aggregator<Integer> aggregator = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });

        DataSchema outputSchema = aggregator.getOutputDataSchema();
        assertEquals(Integer.class, outputSchema.getDataType(0).type);
        assertEquals(Long.class, outputSchema.getDataType(1).type);
        assertEquals(Double.class, outputSchema.getDataType(2).type);
        assertEquals(Double.class, outputSchema.getDataType(3).type);
        assertEquals(Long.class, outputSchema.getDataType(4).type);
        assertEquals(Long.class, outputSchema.getDataType(5).type);
        assertEquals(Double.class, outputSchema.getDataType(6).type);

        Aggregator<Integer> globalAggregator  = aggregator.generateGlobalAggregator();

        assertEquals(outputSchema, globalAggregator.getOutputDataSchema());


    }

    @Test
    public void testHybridAggregation() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        PartialQueryResult partialQueryResult1 = new PartialQueryResult();
        PartialQueryResult partialQueryResult2 = new PartialQueryResult();
        partialQueryResult1.add(new DataTuple(1.0, 1.0, 1L));
        partialQueryResult2.add(new DataTuple(2.0, 2.0, 2L));
        partialQueryResult1.add(new DataTuple(2.0, 1.0, 2L));
        partialQueryResult2.add(new DataTuple(3.0, 3.0, 2L));
        partialQueryResult1.add(new DataTuple(3.0, 1.0, 3L));

        Aggregator<Integer> localAggregator1 = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });

        Aggregator<Integer> localAggregator2 = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });

        Aggregator.IntermediateResult intermediateResult1 = localAggregator1.createIntermediateResult();
        Aggregator.IntermediateResult intermediateResult2= localAggregator2.createIntermediateResult();
        localAggregator1.aggregate(partialQueryResult1.dataTuples, intermediateResult1);
        localAggregator2.aggregate(partialQueryResult2.dataTuples, intermediateResult2);

        Aggregator<Integer> globalAggregator = localAggregator1.generateGlobalAggregator();
        Aggregator.IntermediateResult globalIntermediateResult = globalAggregator.createIntermediateResult();

        globalAggregator.aggregate(localAggregator1.getResults(intermediateResult1).dataTuples, globalIntermediateResult);
        globalAggregator.aggregate(localAggregator2.getResults(intermediateResult2).dataTuples, globalIntermediateResult);

        PartialQueryResult result = globalAggregator.getResults(globalIntermediateResult);
        Collections.sort(result.dataTuples, (DataTuple t1, DataTuple t2) -> ((Comparable)t1.get(0)).compareTo(t2.get(0)) );
        assertEquals(result.dataTuples.get(0), new DataTuple(1.0, 1.0, 1.0, 1.0, 1L, 1L, 1.0));
        assertEquals(result.dataTuples.get(1), new DataTuple(2.0, 2.0, 3.0, 2.0, 2L, 2L, 4.0));
        assertEquals(result.dataTuples.get(2), new DataTuple(3.0, 2.0, 4.0, 3.0, 3L, 2L, 5.0));
    }

    @Test
    public void testHybridAggregationEmptyInput() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        PartialQueryResult partialQueryResult1 = new PartialQueryResult();
        PartialQueryResult partialQueryResult2 = new PartialQueryResult();

        Aggregator<Integer> localAggregator1 = new Aggregator<>(schema, null, new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });

        Aggregator<Integer> localAggregator2 = new Aggregator<>(schema, null, new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });

        Aggregator.IntermediateResult intermediateResult1 = localAggregator1.createIntermediateResult();
        Aggregator.IntermediateResult intermediateResult2= localAggregator2.createIntermediateResult();
        localAggregator1.aggregate(partialQueryResult1.dataTuples, intermediateResult1);
        localAggregator2.aggregate(partialQueryResult2.dataTuples, intermediateResult2);

        Aggregator<Integer> globalAggregator = localAggregator1.generateGlobalAggregator();
        Aggregator.IntermediateResult globalIntermediateResult = globalAggregator.createIntermediateResult();

        globalAggregator.aggregate(localAggregator1.getResults(intermediateResult1).dataTuples, globalIntermediateResult);
        globalAggregator.aggregate(localAggregator2.getResults(intermediateResult2).dataTuples, globalIntermediateResult);

        PartialQueryResult result = globalAggregator.getResults(globalIntermediateResult);
        assertEquals(1, result.dataTuples.size());
        DataTuple tuple = result.dataTuples.get(0);
        assertEquals(0.0, tuple.get(0));
        assertEquals(null, tuple.get(1));
        assertEquals(null, tuple.get(2));
        assertEquals(null, tuple.get(3));
        assertEquals(null, tuple.get(4));
        assertEquals(null, tuple.get(5));

    }



    @Test
    public void testHybridScalarAggregation() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");


        PartialQueryResult partialQueryResult1 = new PartialQueryResult();
        PartialQueryResult partialQueryResult2 = new PartialQueryResult();
        partialQueryResult1.add(new DataTuple(1.0, 1.0, 1L));
        partialQueryResult2.add(new DataTuple(2.0, 2.0, 2L));
        partialQueryResult1.add(new DataTuple(2.0, 2.0, 2L));
        partialQueryResult2.add(new DataTuple(3.0, 3.0, 2L));
        partialQueryResult1.add(new DataTuple(3.0, 1.0, 3L));

        Aggregator localAggregator1 = new Aggregator<>(schema, null,
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")
        );

        Aggregator localAggregator2 = new Aggregator<>(schema, null,
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")
        );

        Aggregator.IntermediateResult intermediateResult1 = localAggregator1.createIntermediateResult();
        Aggregator.IntermediateResult intermediateResult2= localAggregator2.createIntermediateResult();
        localAggregator1.aggregate(partialQueryResult1.dataTuples, intermediateResult1);
        localAggregator2.aggregate(partialQueryResult2.dataTuples, intermediateResult2);

        Aggregator globalAggregator = localAggregator1.generateGlobalAggregator();
        Aggregator.IntermediateResult globalIntermediateResult = globalAggregator.createIntermediateResult();

        globalAggregator.aggregate(localAggregator1.getResults(intermediateResult1).dataTuples, globalIntermediateResult);
        globalAggregator.aggregate(localAggregator2.getResults(intermediateResult2).dataTuples, globalIntermediateResult);

        PartialQueryResult result = globalAggregator.getResults(globalIntermediateResult);
        assertEquals(new DataTuple( 5.0, 9.0, 3.0, 3L, 1L, 10.0), result.dataTuples.get(0));
    }


    @Test
    public void testSerialization() {
        DataSchema schema = new DataSchema();
        schema.addIntField("c1");
        schema.addDoubleField("c2");
        schema.addLongField("c3");
        Aggregator<Integer> localAggregator1 = new Aggregator<>(schema, "c1", new AggregateField[]{
                new AggregateField(new Count<>(), "c2"),
                new AggregateField(new Sum<>(), "c2"),
                new AggregateField(new Max<>(), "c2"),
                new AggregateField(new Max<>(), "c3"),
                new AggregateField(new Min<>(), "c3"),
                new AggregateField(new Sum<>(), "c3")

        });
        byte[] bytes = SerializationUtils.serialize(localAggregator1);
        assertTrue(bytes.length > 0);
    }



}
