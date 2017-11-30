package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInPolygon implements CheckContain {

    private Polygon polygon;
    public CheckInPolygon(Polygon polygon) {
        this.polygon = polygon;

    }

    @Override
    public boolean checkIn(Point point) {
        if (polygon.contains(point)) {
            return true;
        }
        return false;
    }
}
