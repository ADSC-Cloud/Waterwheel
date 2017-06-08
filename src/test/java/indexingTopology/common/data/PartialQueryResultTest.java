package indexingTopology.common.data;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by robert on 8/3/17.
 */
public class PartialQueryResultTest {
    @Test
    public void CapacityTest() {
        PartialQueryResult result = new PartialQueryResult(2);
        assertTrue(result.add(new DataTuple()));
        assertTrue(result.add(new DataTuple()));
        assertFalse(result.add(new DataTuple()));
    }

    @Test
    public void CompactTest() {
        PartialQueryResult largeResult = new PartialQueryResult(Integer.MAX_VALUE);
        final int numberOfTuples = 100000;
        for(int i = 0; i < numberOfTuples; i++) {
            largeResult.add(new DataTuple(i));
        }

        List<PartialQueryResult> compactedResults = PartialQueryResult.Compact(largeResult, 1024);

        int value = 0;
        for(PartialQueryResult compactedResult: compactedResults) {
            for(DataTuple dataTuple: compactedResult.dataTuples) {
                assertEquals(value++, dataTuple.get(0));
            }
        }
        assertEquals(numberOfTuples, value);
    }
}
