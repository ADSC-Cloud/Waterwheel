package indexingTopology.util;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by robert on 9/2/17.
 */
public interface DataTuplePredicate extends Predicate<DataTuple>, Serializable{

public static void main(String[] args) {

    DataSchema dataSchema = new DataSchema();
    dataSchema.addLongField("long");
    dataSchema.addDoubleField("double");
    DataTuplePredicate myPredicate = new DataTuplePredicate() {
        @Override
        public boolean test(DataTuple objects) {
            return (long)objects.get(0) < 5;
        }
    };

    List<DataTuple> tuples = new ArrayList<DataTuple>();
    tuples.add(new DataTuple(0L, 3.3));
    tuples.add(new DataTuple(6L, 4.4));

    for(DataTuple tuple: tuples) {
        if (myPredicate.test(tuple)) {
            System.out.println("succeed:" + tuple.get(0));
        }
    }

    byte[] bytes = SerializationUtils.serialize(myPredicate);
    DataTuplePredicate deserilizedPredicate = (DataTuplePredicate)SerializationUtils.deserialize(bytes);

    for(DataTuple tuple: tuples) {
        if (deserilizedPredicate.test(tuple)) {
            System.out.println("succeed:" + tuple.get(0));
        }
    }

    Number number1 = new Double(10.5);
    Number number2 = 100L;
    if (number1.doubleValue() < number2.doubleValue()) {
        System.out.println("number1 < number2");
    } else
        System.out.println("Otherwise!");


    }
}




