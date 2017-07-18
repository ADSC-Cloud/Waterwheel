package indexingTopology.util.taxi;

import org.junit.Test;
import static org.junit.Assert.*;

public class TrajectoryMovingGeneratorBoltTest {
    @Test
    public void coordinatorBoundTest() throws Exception {
        final double x1 = 0.0;
        final double x2 = 1.0;
        final double y1 = 10.0;
        final double y2 = 11.0;

        TrajectoryMovingGenerator generator = new TrajectoryMovingGenerator(x1, x2, y1, y2, 100, 15.0);
        int testCount = 10000000;
        while(testCount-- > 0) {
            Car car = generator.generate();
            assertTrue(car.x >= x1);
            assertTrue(car.x <= x2);
            assertTrue(car.y >= y1);
            assertTrue(car.y <= y2);
        }
    }
}
