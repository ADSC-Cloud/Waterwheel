package indexingTopology.common.aggregator;

/**
 * Created by robert on 10/3/17.
 */
public class Max<V extends Number & Comparable<V>> implements AggregationFunction<V, V>  {
    @Override
    public V aggregateFunction(V value, V originalA) {
        if (originalA == null || value == null) {
            return originalA == null ? value: originalA;
        }
        return value.doubleValue() > originalA.doubleValue() ? value : originalA;
    }

    @Override
    public V init() {
        return null;
    }

}
