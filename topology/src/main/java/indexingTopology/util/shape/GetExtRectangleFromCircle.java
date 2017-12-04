package indexingTopology.util.shape;

/**
 * Create by zelin on 17-12-4
 **/
public class GetExtRectangleFromCircle implements GetExtRectangle {

    double longitude;
    double latitude;
    double radius;
    public GetExtRectangleFromCircle(double longitude, double latitude, double radius) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.radius = radius;
    }

    @Override
    public Point[] getExtRectangle() {
        Point[] points = new Point[2];
        points[0] = new Point(longitude - radius, latitude + radius);
        points[1] = new Point(longitude + radius, latitude - radius);
        return points;
    }
}
