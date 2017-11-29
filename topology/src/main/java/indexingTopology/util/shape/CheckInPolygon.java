package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInPolygon implements CheckContain {

    private Polygon polygon;
    public CheckInPolygon() {
        polygon = Polygon.Builder()
                .addVertex(new Point(1, 3))
                .addVertex(new Point(2, 8))
                .addVertex(new Point(5, 4))
                .addVertex(new Point(5, 9))
                .addVertex(new Point(7, 5))
                .addVertex(new Point(6, 1))
                .addVertex(new Point(3, 1))
                .build();

    }

    @Override
    public boolean checkIn(Point point) {
        if (polygon.contains(point)) {
            return true;
        }
        return false;
    }
}
