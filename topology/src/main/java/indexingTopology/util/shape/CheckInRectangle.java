package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInRectangle implements CheckContain {

    private double leftTopX, leftTopY;
    private double rightBottomX, rightBottomY;
    public CheckInRectangle() {
        this.leftTopX = 113.325;
        this.leftTopY = 25.1728;
        this.rightBottomX = 115.325;
        this.rightBottomY = 23.1728;
    }

    @Override
    public boolean checkIn(Point point) {
        double pointX = point.x, pointY = point.y;
        if((pointX < rightBottomX && pointX > leftTopX) && (pointY < leftTopY && pointY > rightBottomY)) {
            return true;
        }
        return false;
    }
}
