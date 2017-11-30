package indexingTopology.util.shape;

import junit.framework.TestCase;
import org.junit.Test;


/**
 * Create by zelin on 17-11-28
 **/
public class ContainOrNotTest extends TestCase {

    @Test
    public void testJudgShapeContain() throws Exception {
        Point rectanglePoint = new Point(114.0, 24);
        Point polygonPoint = new Point(5, 7);
        Point circlePoint = new Point(4.5, 7);

        boolean isRectangle = new CheckInRectangle().checkIn(rectanglePoint);
        assertEquals(true, isRectangle);

        boolean isPolygon = new CheckInPolygon().checkIn(polygonPoint);
        assertEquals(true, isPolygon);

        boolean isCircle = new CheckInCircle().checkIn(circlePoint);
        assertEquals(true, isCircle);

    }
}
