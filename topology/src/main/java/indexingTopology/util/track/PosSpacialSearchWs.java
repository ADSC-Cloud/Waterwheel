package indexingTopology.util.track;

import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.util.shape.Circle;
import indexingTopology.util.shape.Point;
import indexingTopology.util.shape.Polygon;
import indexingTopology.util.shape.Rectangle;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.w3c.dom.css.Rect;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by zelin on 17-12-15
 **/
public class PosSpacialSearchWs {

    private String QueryServerIp = "localhost";
    private Point leftTop, rightBottom;
    private Point[] geoStr;
    private Point circle;
    private double radius;
    private Point externalLeftTop, externalRightBottom;

    public String service(String permissionsParams, String businessParams) {
        DataSchema schema = getDataSchema();
        JSONObject jsonObject = JSONObject.fromObject(businessParams);
        String type = jsonObject.getString("type");
        Pattern p = null;
        boolean flag = true;
//        System.out.println(geoArray.toString());
        DataTuplePredicate predicate = null;
        switch (type) {
            case "rectangle" : {
                p = Pattern.compile("^\\-?[0-9]+\\.?[0-9]*+\\,\\-?[0-9]+\\.?[0-9]*");
                String rectLeftTop = jsonObject.get("leftTop").toString();
                boolean b1 = p.matcher(rectLeftTop).matches();
                String rectRightBottom = jsonObject.get("rightBottom").toString();
                boolean b2 = p.matcher(rectRightBottom).matches();
                if (!b1 || !b2) {
                    flag = false;
                    break;
                }
                Rectangle rectangle = initRectangel(rectLeftTop, rectRightBottom);
                externalLeftTop = new Point(rectangle.getExternalRectangle().getLeftTopX(), rectangle.getExternalRectangle().getLeftTopY());
                externalRightBottom = new Point(rectangle.getExternalRectangle().getRightBottomX(), rectangle.getExternalRectangle().getRightBottomY());
                if (externalLeftTop.x > externalRightBottom.x || externalLeftTop.y < externalRightBottom.y) {
                    JSONObject queryResponse = new JSONObject();
                    queryResponse.accumulate("success", false);
                    queryResponse.accumulate("result", null);
                    queryResponse.accumulate("errorCode", 1002);
                    queryResponse.accumulate("errorMsg", "参数值无效或缺失必填参数");
                    System.out.println(queryResponse);
                    return queryResponse.toString();
                }
                predicate = t -> rectangle.checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));
                break;
            }
            case "polygon" : {
                JSONArray geoArray = null;
                if (jsonObject.getJSONArray("geoStr") != null) {
                    geoArray = jsonObject.getJSONArray("geoStr");
                }else {
                    flag = false;
                    break;
                }
                Polygon polygon = initPolygon(geoArray);
                externalLeftTop = new Point(polygon.getExternalRectangle().getLeftTopX(), polygon.getExternalRectangle().getLeftTopY());
                externalRightBottom = new Point(polygon.getExternalRectangle().getRightBottomX(), polygon.getExternalRectangle().getRightBottomY());
                predicate = t -> polygon.checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));
                break;
            }
            case "circle" : {
                p = Pattern.compile("^\\-?[0-9]+\\.?[0-9]*");
                String longitude = jsonObject.get("longitude").toString();
                boolean b1 = p.matcher(longitude).matches();
                String latitude = jsonObject.get("latitude").toString();
                boolean b2 = p.matcher(latitude).matches();
                String circleradius = jsonObject.get("radius").toString();
                boolean b3 = p.matcher(circleradius).matches();
                if (!b1 || !b2 || !b3) {
                    flag = false;
                    break;
                }
                Circle circle = initCircle(longitude, latitude, circleradius);
                externalLeftTop = new Point(circle.getExternalRectangle().getLeftTopX(), circle.getExternalRectangle().getLeftTopY());
                externalRightBottom = new Point(circle.getExternalRectangle().getRightBottomX(), circle.getExternalRectangle().getRightBottomY());
                predicate = t -> circle.checkIn(new Point((Double)schema.getValue("lon", t),(Double)schema.getValue("lat", t)));
                break;
            }

            default: break;
        }
        JSONObject queryResponse = new JSONObject();
        if (flag == true) {
            final double xLow = externalLeftTop.x;
            final double xHigh =externalRightBottom.x;
            final double yLow = externalRightBottom.y;
            final double yHigh =externalLeftTop.y;
            JSONArray queryResult = null;
            GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, 10001);
            try {
                queryClient.connectWithTimeout(10000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(xLow, xHigh, yLow, yHigh,
                    System.currentTimeMillis() - 120 * 1000,
                    System.currentTimeMillis(), predicate, null,null, null, null);
            try {
                QueryResponse response = queryClient.query(queryRequest);
                List<DataTuple> tuples = response.getTuples();
                queryResult = new JSONArray();
                for (DataTuple tuple : tuples){
                    queryResult.add(schema.getJsonFromDataTuple(tuple));
                    System.out.println(tuple.toValues());
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            queryResponse.accumulate("success", true);
            queryResponse.accumulate("result", queryResult);
            queryResponse.accumulate("errorCode", null);
            queryResponse.accumulate("errorMsg", null);
        }else{
            queryResponse.accumulate("success", false);
            queryResponse.accumulate("result", null);
            queryResponse.accumulate("errorCode","1001");
            queryResponse.accumulate("errorMsg", "参数解析失败，参数格式存在问题");
        }
        System.out.println(queryResponse);
        return queryResponse.toString();
    }

    Polygon initPolygon(JSONArray geoArray) {
        int size = geoArray.size();
        geoStr = new Point[size];
        for (int i = 0; i < size; i++) {
            String[] strings = geoArray.get(i).toString().split(" ");
            geoStr[i] = new Point(Double.parseDouble(strings[0]), Double.parseDouble(strings[1]));
        }
        Polygon.Builder polygonBuilder = Polygon.Builder();
        for (Point point : geoStr) {
            polygonBuilder.addVertex(point);
        }
        return polygonBuilder.build();
    }

    Circle initCircle(String longitude, String latitude, String radius) {
        double circlelon = Double.parseDouble(longitude);
        double circlelat = Double.parseDouble(latitude);
        double circlerad = Double.parseDouble(radius);
        Circle circle = new Circle(circlelon, circlelat, circlerad);
        return circle;
    }

    Rectangle initRectangel(String leftTop, String rightBottom) {
        double leftTop_x = Double.parseDouble(leftTop.split(",")[0]);
        double leftTop_y = Double.parseDouble(leftTop.split(",")[1]);
        double rightBottom_x = Double.parseDouble(rightBottom.split(",")[0]);
        double rightBottom_y = Double.parseDouble(rightBottom.split(",")[1]);
        Point rectLeftTop = new Point(leftTop_x, leftTop_y);
        Point rectRightBottom = new Point(rightBottom_x, rightBottom_y);
        Rectangle rectangle = new Rectangle(rectLeftTop, rectRightBottom);
        return rectangle;
    }

    static private DataSchema getDataSchema() {
        DataSchema rawSchema = new DataSchema();
        rawSchema.addDoubleField("lon");
        rawSchema.addDoubleField("lat");
        rawSchema.addIntField("devbtype");
        rawSchema.addVarcharField("devid", 8);
        rawSchema.addVarcharField("id", 32);
        return rawSchema;
    }




    public static void main(String[] args) {

        String searchTest = "{\"type\":\"rectangle\",\"leftTop\":\"80,100\",\"rightBottom\":\"90,80\",\"geoStr\":null,\"lon\":null,\"lat\":null,\"radius\":null}";
        String searchTest2 = "{\"type\":\"circle\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":null,\"lon\":85,\"lat\":75,\"radius\":5}";
        String searchTest3 = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":null,\"lon\":null,\"lat\":null,\"radius\":null}";
        String businessParams = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":[\"1 3\",\"2 8\",\"5 4\",\"5 9\",\"7 5\"],\"longitude\":null,\"latitude\":null,\"radius\":null}";
        PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
        posSpacialSearchWs.service(null, searchTest);
    }
}
