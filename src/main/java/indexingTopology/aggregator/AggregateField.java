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
    public String aggregateFieldName() {
        if (function instanceof Count) {
            return String.format("count(%s)", fieldName);
        } else if (function instanceof Sum) {
            return String.format("sum(%s)", fieldName);
        } else if (function instanceof Min) {
            return String.format("min(%s)", fieldName);
        } else if (function instanceof Max) {
            return String.format("max(%s)", fieldName);
        }
        return null;
    }
}
