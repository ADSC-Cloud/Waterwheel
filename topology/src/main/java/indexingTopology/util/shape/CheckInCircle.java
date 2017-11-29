package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInCircle implements CheckContain {

    private double longitude;
    private double latitude;
    private double radius;
    public CheckInCircle() {
        this.longitude = 113.325;
        this.latitude = 23.1728;
        this.radius = 1000.0;
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
