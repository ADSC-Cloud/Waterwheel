package indexingTopology.common.logics;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by Robert on 5/8/17.
 */
public class DataTupleEquivalentPredicateHint implements Predicate<DataTuple>, Serializable{
    public String column;
    public Object value;
    DataSchema schema;

    public DataTupleEquivalentPredicateHint(String column, Object value) {
        this.column = column;
        this.value = value;
    }

    @Override
    public boolean test(DataTuple objects) {
        return schema.getValue(column, objects).equals(value);
    }
}
