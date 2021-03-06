package indexingTopology.util;

import indexingTopology.common.data.DataTuple;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by Robert on 3/10/17.
 */
public class DataTupleTest {
    @Test
    public void testEquals() {
        assertEquals(new DataTuple(0.1, 2, 4L, "hello"), new DataTuple(0.1, 2, 4L, "hello"));
        assertTrue(new DataTuple(0.1, 2, 4L, "hello").equals(new DataTuple(0.1, 2, 4L, "hello")));
        assertFalse(new DataTuple(0.1, 2, 4L, "hello").equals(new DataTuple(0.2, 2, 4L, "hello")));
    }
}
