package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInRectangle implements CheckContain {

    private double leftTopX, leftTopY;
    private double rightBottomX, rightBottomY;
    public CheckInRectangle(Point leftTop, Point rightBottom) {
        this.leftTopX = leftTop.x;
        this.leftTopY = leftTop.y;
        this.rightBottomX = rightBottom.x;
        this.rightBottomY = rightBottom.y;
    }

    @Override
    public boolean checkIn(Point point) {
        double pointX = point.x, pointY = point.y;
        if((pointX <= rightBottomX && pointX >= leftTopX) && (pointY <= leftTopY && pointY >= rightBottomY)) {
            return true;
        }
        return false;
    }
}
