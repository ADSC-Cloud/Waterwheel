package indexingTopology.aggregator;

import java.io.Serializable;

/**
 * Created by robert on 10/3/17.
 */
public class AggregateField implements Serializable {
    public AggregationFunction function;
    public String fieldName;
    public AggregateField(AggregationFunction function, String fieldName) {
        this.fieldName = fieldName;
        this.function = function;
    }
}
