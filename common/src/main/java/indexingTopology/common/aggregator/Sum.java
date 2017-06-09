package indexingTopology.common.aggregator;

/**
 * Created by robert on 10/3/17.
 */
public class Sum<I extends Number> implements AggregationFunction<I, Double> {

    @Override
    public Double aggregateFunction(I value, Double originalA) {
        if (value == null || originalA == null) {
            if (value == null)
                return originalA;
            else
                return value.doubleValue();
        }
        return (value.doubleValue() + originalA);
    }

    @Override
    public Double init() {
        return null;
    }
}
