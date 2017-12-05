package indexingTopology.util.shape;

/**
 * Create by zelin on 17-12-5
 **/
public class Circle implements Shape{

    private double longitude;
    private double latitude;
    private double radius;

    public Circle(double longitude, double latitude, double radius) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.radius = radius;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getRadius() {
        return radius;
    }

    @Override
    public boolean checkIn(Point point) {
        double pointX = point.x, pointY = point.y;
        double len = Math.sqrt(Math.pow(pointX - longitude, 2) + Math.pow(pointY - latitude, 2));
        if(radius >= len) {
            return true;
        }
        return false;
    }

    @Override
    public Rectangle getExternalRectangle() {
        Point leftTop = new Point(longitude - radius, latitude + radius);
        Point rightBottom = new Point(longitude + radius, latitude - radius);
        return new Rectangle(leftTop, rightBottom);
    }
}
