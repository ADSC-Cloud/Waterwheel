package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInCircle implements CheckContain {

    private double longitude;
    private double latitude;
    private double radius;
    public CheckInCircle(double longitude, double latitude, double radius) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.radius = radius;
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
}
