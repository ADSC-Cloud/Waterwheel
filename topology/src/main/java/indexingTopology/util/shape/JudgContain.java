package indexingTopology.util.shape;

import com.google.gson.JsonObject;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;


import java.util.ArrayList;
import java.util.List;

import static com.github.davidmoten.rtree.fbs.generated.GeometryType_.Point;

/**
 * Create by zelin on 17-11-27
 **/
public class JudgContain {

    public static List<DataTuple> checkInPolygon(List<DataTuple> list, DataTuple polygonTuple) {
        List<DataTuple> outList = new ArrayList<>();
        String[] geoStr = polygonTuple.get(3).toString().split(" ");
        Polygon polygon = builtPolygon(geoStr.length, geoStr);
        for (DataTuple object : list) {
            Double pointX = (Double) object.get(4), pointY = (Double) object.get(5);
            System.out.println(pointX + "ddddd" + pointY);
            Point point = new Point(pointX, pointY);
            if(polygon.contains(point)) {
                outList.add(object);
            }
        }
        return outList;
    }

    public static List<DataTuple> checkInRect(List<DataTuple> list, DataTuple rectTuple) {
        List<DataTuple> outList = new ArrayList<>();
        String[] leftTop = rectTuple.get(1).toString().split("/");
        String[] rightBottom = rectTuple.get(2).toString().split("/");
        Double leftTopX = Double.valueOf(leftTop[0]), leftTopY = Double.valueOf(leftTop[1]);
        Double rightBottomX = Double.valueOf(rightBottom[0]), rightBottomY = Double.valueOf(rightBottom[1]);
        for (DataTuple object : list) {
            Double pointX = (Double) object.get(4), pointY = (Double) object.get(5);
            System.out.println(pointX + "ddddd" + pointY);
            if((pointX < rightBottomX && pointX > leftTopX) && (pointY < leftTopY && pointY > rightBottomY)) {
                outList.add(object);
            }
        }
//        double x1 = 113.325, x2 = 115.325;
//        double y1 = 25.1728, y2 = 23.1728;
//        double x = 114.0, y = 25.0;
//        if((x > x1 && x < x2) && (y < y1 && y > y2)) {
//            return true;
//        }
        return outList;
    }

    public static List<DataTuple> checkInCircle(List<DataTuple> list, DataTuple circleTuple) {
        List<DataTuple> outList = new ArrayList<>();
        Double circleLongitude = Double.parseDouble(circleTuple.get(4).toString());
        Double circleLatitude = Double.parseDouble(circleTuple.get(5).toString());
        Double circleRadius = Double.parseDouble(circleTuple.get(6).toString());
        for(DataTuple object : list) {
            Double pointX = (Double) object.get(4), pointY = (Double) object.get(5);
            System.out.println(pointX + "ddddd" + pointY);
            double len = Math.sqrt(Math.pow(pointX - circleLongitude, 2) + Math.pow(pointY - circleLatitude, 2));
            if(circleRadius >= len)
                outList.add(object);
        }
        return outList;
    }

    public static void main(String[] args) {
        JudgContain judgContain = new JudgContain();
//        System.out.println(judgContain.checkInPolygon());
//        DataSchema dataSchema = JudgContain.init();
//
////        System.out.println(judgContain.checkInRect());
//        System.out.println(judgContain.checkInCircle());
        String str = "1/3 5/4 2/8 5/9 7/5 6/1 3/1";
        String[] geoStr = str.split(" ");
        for (String str1 : geoStr) {
            System.out.println(str1);
        }
    }

    public static DataSchema init() {
        DataSchema dataSchema = new DataSchema();
        dataSchema.addVarcharField("rectangle", 8);
        dataSchema.addVarcharField("leftTop",15);
        dataSchema.addVarcharField("rightBottom", 15);
        dataSchema.addVarcharField("geoStr", 30);
        dataSchema.addDoubleField("longitude");
        dataSchema.addDoubleField("latitude");
        dataSchema.addDoubleField("radius");
        return dataSchema;
    }

    private static Polygon builtPolygon(int arraylen, String[] strings) {
        Polygon polygon = null;
        Point[] points = new Point[strings.length];
        for (int i = 0; i < strings.length; i++) {
            points[i] = new Point(Double.parseDouble(strings[i].split("/")[0]),Double.parseDouble(strings[i].split("/")[1]));
        }
        switch (arraylen) {
            case 4: polygon = Polygon.Builder()
                    .addVertex(points[0])
                    .addVertex(points[1])
                    .addVertex(points[2])
                    .addVertex(points[3])
                    .build();break;
            case 5: polygon = Polygon.Builder()
                    .addVertex(points[0])
                    .addVertex(points[1])
                    .addVertex(points[2])
                    .addVertex(points[3])
                    .addVertex(points[4])
                    .build();break;
            case 6: polygon = Polygon.Builder()
                    .addVertex(points[0])
                    .addVertex(points[1])
                    .addVertex(points[2])
                    .addVertex(points[3])
                    .addVertex(points[4])
                    .addVertex(points[5])
                    .build();break;
            case 7: polygon = Polygon.Builder()
                    .addVertex(points[0])
                    .addVertex(points[1])
                    .addVertex(points[2])
                    .addVertex(points[3])
                    .addVertex(points[4])
                    .addVertex(points[5])
                    .addVertex(points[6])
                    .build();break;
            case 8: polygon = Polygon.Builder()
                    .addVertex(points[0])
                    .addVertex(points[1])
                    .addVertex(points[2])
                    .addVertex(points[3])
                    .addVertex(points[4])
                    .addVertex(points[5])
                    .addVertex(points[6])
                    .addVertex(points[7])
                    .build();break;
        }
        return polygon;
    }

}
