package indexingTopology.aggregator;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.PartialQueryResult;
import org.junit.Test;

import java.util.Collections;

import static junit.framework.TestCase.*;
/**
 * Created by Robert on 3/10/17.
 */
public class AggregationTest {

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
        aggregator.aggregate(partialQueryResult.dataTuples);


        PartialQueryResult result = aggregator.getResults();
        System.out.println(result);
        assertEquals(result.dataTuples.get(0), new DataTuple(1, 1L, 180.0, 180, 180));
        assertEquals(result.dataTuples.get(1), new DataTuple(2, 2L, 348.0, 176, 172));
        assertEquals(result.dataTuples.get(2), new DataTuple(3, 2L, 350.0, 183, 167));

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
        aggregator.aggregate(partialQueryResult.dataTuples);


        PartialQueryResult result = aggregator.getResults();
        System.out.println(result);
//        Collections.sort(result.dataTuples, (DataTuple t1, DataTuple t2) -> t1.get(0).compareTo(t2.get(0)) );
        assertEquals(result.dataTuples.get(0), new DataTuple(1.0, 1L, 1.0, 1.0, 1L, 1L, 1.0));
        assertEquals(result.dataTuples.get(1), new DataTuple(2.0, 2L, 3.0, 2.0, 2L, 2L, 4.0));
        assertEquals(result.dataTuples.get(2), new DataTuple(3.0, 2L, 4.0, 3.0, 3L, 2L, 5.0));

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
        aggregator.aggregate(partialQueryResult.dataTuples);


        PartialQueryResult result = aggregator.getResults();
        System.out.println(result);
        assertEquals(result.dataTuples.get(0), new DataTuple(1, 1L, 1.0, 1.0, 1L, 1L, 1.0));
        assertEquals(result.dataTuples.get(1), new DataTuple(2, 2L, 3.0, 2.0, 2L, 2L, 4.0));
        assertEquals(result.dataTuples.get(2), new DataTuple(3, 2L, 4.0, 3.0, 3L, 2L, 5.0));

    }

}
