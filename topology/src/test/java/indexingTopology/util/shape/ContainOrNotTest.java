package indexingTopology.util.shape;

import junit.framework.TestCase;
import org.junit.Test;



/**
 * Create by zelin on 17-11-28
 **/
public class ContainOrNotTest extends TestCase {

    @Test
    public void testRectangleContain() throws Exception {
        Point rectanglePoint;
        boolean isRectangle;
        //in
        rectanglePoint = new Point(1, 1);
        isRectangle = new CheckInRectangle(new Point(0, 2), new Point (2, 0)).checkIn(rectanglePoint);
        assertEquals(true, isRectangle);
        //out
        rectanglePoint = new Point(0, 3);
        isRectangle = new CheckInRectangle(new Point(0, 2), new Point (2, 0)).checkIn(rectanglePoint);
        assertEquals(false, isRectangle);
        //edge
        rectanglePoint = new Point(1, 2);
        isRectangle = new CheckInRectangle(new Point(0, 2), new Point (2, 0)).checkIn(rectanglePoint);
        assertEquals(true, isRectangle);
    }

    @Test
    public void testCircleContain() {
        Point circlePoint;
        boolean isCircle;
        //in
        circlePoint = new Point(1, 0);
        isCircle = new CheckInCircle(1, 0, 1).checkIn(circlePoint);
        assertEquals(true, isCircle);
        //out
        circlePoint = new Point(1, 2);
        isCircle = new CheckInCircle(1, 0, 1).checkIn(circlePoint);
        assertEquals(false, isCircle);
        //edge
        circlePoint = new Point(1, 1);
        isCircle = new CheckInCircle(1, 0, 1).checkIn(circlePoint);
        assertEquals(true, isCircle);
    }

    @Test
    public void testPolygonContain() {
        Point polygonPoint;
        //the start and the end must the same point
        Polygon polygon= Polygon.Builder()
                .addVertex(new Point(1, 3))
                .addVertex(new Point(2, 8))
                .addVertex(new Point(5, 4))
                .addVertex(new Point(5, 9))
                .addVertex(new Point(7, 5))
                .addVertex(new Point(6, 1))
                .addVertex(new Point(3, 1))
                .addVertex(new Point(1, 3))
                .build();
        boolean isPolygon;
        //in
        polygonPoint  = new Point(1,3);
        isPolygon = new CheckInPolygon(polygon).checkIn(polygonPoint);
        assertEquals(true, isPolygon);
        //out
        polygonPoint  = new Point(0,0);
        isPolygon = new CheckInPolygon(polygon).checkIn(polygonPoint);
        assertEquals(false, isPolygon);
        //edge
        polygonPoint  = new Point(2,2);
        isPolygon = new CheckInPolygon(polygon).checkIn(polygonPoint);
        assertEquals(true, isPolygon);
    }
}
