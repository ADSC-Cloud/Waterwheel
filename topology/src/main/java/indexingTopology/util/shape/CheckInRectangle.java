package indexingTopology.util.shape;

/**
 * Create by zelin on 17-11-29
 **/
public class CheckInRectangle implements CheckContain {

    private double leftTopX, leftTopY;
    private double rightBottomX, rightBottomY;
    public CheckInRectangle(double leftTopX, double leftTopY, double rightBottomX, double rightBottomY) {
        this.leftTopX = leftTopX;
        this.leftTopY = leftTopY;
        this.rightBottomX = rightBottomX;
        this.rightBottomY = rightBottomY;
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
