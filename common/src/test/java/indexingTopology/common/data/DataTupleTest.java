package indexingTopology.common.data;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Robert on 8/2/17.
 */
public class DataTupleTest {

    @Test
    public void toValues() throws Exception {
        DataTuple tuple = new DataTuple(100, 2000L, "Waterwheel");
        assertEquals("[100, 2000, Waterwheel]", tuple.toValues().toString());
    }

}