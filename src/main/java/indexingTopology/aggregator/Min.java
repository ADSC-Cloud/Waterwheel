package indexingTopology.aggregator;

/**
 * Created by robert on 10/3/17.
 */
public class Min<V extends Number & Comparable<V>> implements AggregationFunction<V, V>  {
    @Override
    public V aggregateFunction(V value, V originalA) {

        return value.doubleValue() < originalA.doubleValue() ? value : originalA;
    }

    @Override
    public V init() {
//        if(this.getClass() == Max<Double>) {
//
//        }
        return (V)new Double(Double.MAX_VALUE);
    }

}
