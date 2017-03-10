package indexingTopology.aggregator;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.data.PartialQueryResult;

/**
 * Created by robert on 10/3/17.
 */
public class AggregationTest {

    static public void main(String[] args) {
        DataSchema schema = new DataSchema();
        schema.addIntField("id");
        schema.addIntField("group");
        schema.addIntField("height");


        PartialQueryResult partialQueryResult = new PartialQueryResult();
        partialQueryResult.add(new DataTuple(1, 1, 180.0));
        partialQueryResult.add(new DataTuple(2, 2, 176.2));
        partialQueryResult.add(new DataTuple(3, 2, 172.4));
        partialQueryResult.add(new DataTuple(4, 3, 183.5));
        partialQueryResult.add(new DataTuple(5, 3, 167.7));

        Aggregator<Integer> aggregator = new Aggregator<>(schema, "group", new AggregateField[]{
                new AggregateField(new Count<>(), "group"),
                new AggregateField(new Sum<>(), "height"),
                new AggregateField(new Max<Double>(), "height"),
                new AggregateField(new Min<Double>(), "height")

        });
        aggregator.aggregate(partialQueryResult.dataTuples);


        PartialQueryResult result = aggregator.getResults();
        System.out.println(result);
    }
}
