package indexingTopology.util;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import org.junit.Test;
import static junit.framework.TestCase.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by acelzj on 21/2/17.
 */
public class DataTupleMapperTest {

    @Test
    public void testDataTupleMapper() {

        DataSchema schema = new DataSchema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");

        List<DataTuple> dataTupleList = new ArrayList<>();
        dataTupleList.add(new DataTuple(1, 2, 3));
        dataTupleList.add(new DataTuple(4, 5, 6));


        DataTupleMapper mapper = new DataTupleMapper(null, (DataTuple t) -> {
            DataTuple dataTuple = t;
            dataTuple.add((int)schema.getValue("f1", t) + (int)schema.getValue("f2", t) + (int)schema.getValue("f3", t));
            return dataTuple;
        });

    }

}