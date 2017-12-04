package indexingTopology.util.shape;

import java.util.List;

/**
 * Create by zelin on 17-12-4
 **/
public class GetExtRectangleFromPolygon implements GetExtRectangle {

    Polygon polygon;
    public GetExtRectangleFromPolygon(Polygon polygon) {
        this.polygon = polygon;
    }

    @Override
    public Point[] getExtRectangle() {
        List<Point> points = polygon.getVertex();
        if (points.size() < 3) {
            throw new RuntimeException("Polygon must have at least 3 points");
        }
        Point[] rectPoints = new Point[2];
        rectPoints[0] = new Point(points.get(0).x, points.get(0).y);
        rectPoints[1] = new Point(points.get(1).x, points.get(1).y);
        for (int i = 0; i < points.size() - 1; i++) {
            rectPoints[0].x = rectPoints[0].x < points.get(i+1).x?rectPoints[0].x:points.get(i+1).x;
            rectPoints[0].y = rectPoints[0].y > points.get(i+1).y?rectPoints[0].y:points.get(i+1).y;
            rectPoints[1].x = rectPoints[1].x > points.get(i+1).x?rectPoints[1].x:points.get(i+1).x;
            rectPoints[1].y = rectPoints[1].y < points.get(i+1).y?rectPoints[1].y:points.get(i+1).y;
        }
        return rectPoints;
    }
}
