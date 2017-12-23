package indexingTopology.util.shape;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Create by zelin on 17-12-4
 **/
public class GetExternalRectangleTest extends TestCase{

    @Test
    public void testGetRectangleFromPolygon() {
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
        Rectangle rectangle = polygon.getExternalRectangle();
        assertEquals(1.0, rectangle.getLeftTopX());
    }

    @Test
    public void testGetRectangleFromCircle() {
        Circle circle = new Circle(1, 0, 1);
        Rectangle rectangle = circle.getExternalRectangle();
        assertEquals(0.0, rectangle.getLeftTopX());
    }
}
