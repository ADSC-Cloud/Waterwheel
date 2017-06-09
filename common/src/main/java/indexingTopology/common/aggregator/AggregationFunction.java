package indexingTopology.common.aggregator;

import java.io.Serializable;

/**
 * Created by robert on 10/3/17.
 */
public interface AggregationFunction<V, A> extends Serializable{
    A aggregateFunction(V value, A originalA);
    A init();
}
