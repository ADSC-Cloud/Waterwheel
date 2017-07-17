package indexingTopology.util;

import indexingTopology.util.taxi.City;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * Created by robert on 10/7/17.
 */
public class CityTest extends TestCase {
    @Test
    public void testBoundary() {
        double x1 = 100.0;
        double x2 = 200.0;
        double y1 = 150.0;
        double y2 = 300.0;
        int partitions = 128;
        City city = new City(x1, x2, y1, y2, partitions);
        int maxZCode = city.getMaxZCode();
        for (int i = - 1000; i < 1000; i++) {
            for (int j = -1000; j < 1000; j++) {
                assertTrue(city.getZCodeForALocation((double)i, (double)j) <= maxZCode);
                assertTrue(city.getZCodeForALocation((double)i, (double)j) >=0);
            }
        }
    }
}
