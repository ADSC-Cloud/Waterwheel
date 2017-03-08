package indexingTopology.util;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;


/**
 * Created by robert on 9/2/17.
 */
public class DataTuplePredicateTest {
    @Test
    public void PredicateExecutionOnePassedTest() {
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

        List<DataTuple> survivedTuples = new ArrayList<>();
        for (DataTuple tuple: tuples) {
            if (myPredicate.test(tuple)) {
                survivedTuples.add(tuple);
            }
        }

        assertEquals(1, survivedTuples.size());
        assertEquals(0L, survivedTuples.get(0).get(0));
    }

    @Test
    public void PredicateExecutionNonePassedTest() {
        DataSchema dataSchema = new DataSchema();
        dataSchema.addLongField("long");
        dataSchema.addDoubleField("double");
        DataTuplePredicate myPredicate = new DataTuplePredicate() {
            @Override
            public boolean test(DataTuple objects) {
                return (long)objects.get(0) > 100;
            }
        };

        List<DataTuple> tuples = new ArrayList<DataTuple>();
        tuples.add(new DataTuple(0L, 3.3));
        tuples.add(new DataTuple(6L, 4.4));

        List<DataTuple> survivedTuples = new ArrayList<>();
        for (DataTuple tuple: tuples) {
            if (myPredicate.test(tuple)) {
                survivedTuples.add(tuple);
            }
        }

        assertEquals(0, survivedTuples.size());
    }

//    @Test
//    public void SerializationTest() {
//        DataSchema dataSchema = new DataSchema();
//        dataSchema.addLongField("long");
//        dataSchema.addDoubleField("double");
//        DataTuplePredicate myPredicate = new DataTuplePredicate() {
//            @Override
//            public boolean test(DataTuple objects) {
//                return (long)objects.get(0) > 100;
//            }
//        };
//
//        List<DataTuple> tuples = new ArrayList<>();
//        tuples.add(new DataTuple(0L, 3.3));
//        tuples.add(new DataTuple(6L, 4.4));
//        byte[] bytes = SerializationUtils.serialize(myPredicate);
//        DataTuplePredicate deserilizedPredicate = (DataTuplePredicate)SerializationUtils.deserialize(bytes);
//
//        List<DataTuple> survivedTuples = new ArrayList<>();
//        for (DataTuple tuple: tuples) {
//            if (deserilizedPredicate.test(tuple)) {
//                survivedTuples.add(tuple);
//            }
//        }
//
//        assertEquals(0, survivedTuples.size());
//    }
//    public static void main(String[] args) {
//
//        DataSchema dataSchema = new DataSchema();
//        dataSchema.addLongField("long");
//        dataSchema.addDoubleField("double");
//        DataTuplePredicate myPredicate = new DataTuplePredicate() {
//            @Override
//            public boolean test(DataTuple objects) {
//                return (long)objects.get(0) > 100;
//            }
//        };
//
//        List<DataTuple> tuples = new ArrayList<>();
//        tuples.add(new DataTuple(0L, 3.3));
//        tuples.add(new DataTuple(6L, 4.4));
//        byte[] bytes = SerializationUtils.serialize(myPredicate);
//        DataTuplePredicate deserilizedPredicate = (DataTuplePredicate)SerializationUtils.deserialize(bytes);
//
//        List<DataTuple> survivedTuples = new ArrayList<>();
//        for (DataTuple tuple: tuples) {
//            if (deserilizedPredicate.test(tuple)) {
//                survivedTuples.add(tuple);
//            }
//        }
//    }
}
