package indexingTopology.common.aggregator;

/**
 * Created by robert on 10/3/17.
 */
public class Count<T> implements AggregationFunction<T, Long> {

    @Override
    public Long aggregateFunction(T value, Long originalA) {
        return originalA + 1;
    }

    @Override
    public Long init() {
        return 0L;
    }
}
