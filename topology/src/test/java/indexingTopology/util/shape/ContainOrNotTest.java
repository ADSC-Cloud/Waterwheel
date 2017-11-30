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

        boolean isRectangle = new CheckInRectangle(113.325, 25.1728, 115.325, 23.1728).checkIn(rectanglePoint);
        assertEquals(true, isRectangle);

        Polygon polygon = Polygon.Builder()
                .addVertex(new Point(1, 3))
                .addVertex(new Point(2, 8))
                .addVertex(new Point(5, 4))
                .addVertex(new Point(5, 9))
                .addVertex(new Point(7, 5))
                .addVertex(new Point(6, 1))
                .addVertex(new Point(3, 1))
                .build();
        boolean isPolygon = new CheckInPolygon(polygon).checkIn(polygonPoint);
        assertEquals(true, isPolygon);

        boolean isCircle = new CheckInCircle(113.325, 23.1728, 1000.0).checkIn(circlePoint);
        assertEquals(true, isCircle);
    }
}
