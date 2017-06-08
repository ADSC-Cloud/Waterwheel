package indexingTopology.util;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleMapper;
import org.junit.Test;
import static org.junit.Assert.*;
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

        DataTuple t1 = new DataTuple(1, 2, 3);
        DataTuple t2 = new DataTuple(4, 5, 6);


        DataTupleMapper mapper = new DataTupleMapper(null, (DataTuple t) -> {
            DataTuple dataTuple = t;
            dataTuple.add((int)schema.getValue("f1", t) + (int)schema.getValue("f2", t) + (int)schema.getValue("f3", t));
            return dataTuple;
        });
        mapper.map(t1);
        mapper.map(t2);

        assertEquals(6, t1.get(3));
        assertEquals(15, t2.get(3));


    }

}