package indexingTopology.util.track;

import indexingTopology.util.shape.Point;
import indexingTopology.util.shape.Rectangle;
import org.eclipse.jdt.internal.core.SourceType;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import javax.xml.bind.SchemaOutputResolver;
import java.util.Random;

/**
 * Created by zelin on 2017/1/11
 */
public class SearchTestByArgs {
    @Option(name = "--mode", aliases = {"-m"}, usage = "trackSearch|trackPagedSearch|posNonSpacialSearch|posSpacialSearch")
    private String Mode = "Not Given";

    @Option(name = "--time-range", aliases = {"-time"}, usage = "the search time range")
    private long TimeRange = 1000 * 1000;

    @Option(name = "--shape", aliases = {"-s"}, usage = "rectangle|circle|polygon")
    private String Shape = "Not Given";

    @Option(name = "--longitude", aliases = {"-lo"}, usage = "longitude of circle")
    private String Longitude = "113.0";

    @Option(name = "--latitude", aliases = {"-la"}, usage = "latitude of circle")
    private String Latitude = "23.0";

    @Option(name = "--radius", usage = "radius of circle")
    private String Radius = "1.0";

    @Option(name = "--lefttop", usage = "lefttop of rectangle")
    private String LeftTop = "111.012928,25.292677";

    @Option(name = "--rightbottom", usage = "rightbottom of rectangle")
    private String RightBottom = "115.023983,21.614865";

    @Option(name = "--geostr", usage = "geostr of polygon")
    private String Geostr = "Not Given";

    @Option(name = "--page", usage = "page of page search")
    private String Page = "1";

    @Option(name = "--row", usage = "row of page search")
    private String Row = "10";

    @Option(name = "--city", aliases = {"-c"}, usage = "row of page search")
    private String City = "4401";

    @Option(name = "--devbtype", aliases = {"-devb"}, usage = "row of page search")
    private String Devbtype = "10";

    @Option(name = "--devid", aliases = {"-devid"}, usage = "row of page search")
    private String Devid = "0x0101";

    @Option(name = "--percent", aliases = {"-p"}, usage = "percentage of the shape search")
    private String Percent = "-1";

    @Option(name = "--id", usage = "query over id")
    private String id = null;


    static final double x1 = 111.012928;
    static final double x2 = 115.023983;
    static final double y1 = 25.292677;
    static final double y2 = 21.614865;

    double leftTop_x;
    double leftTop_y;
    double rightBottom_x;
    double rightBottom_y;

    public static void main(String[] args) {

        SearchTestByArgs SearchTestByArgs = new SearchTestByArgs();

        CmdLineParser parser = new CmdLineParser(SearchTestByArgs);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        long start = System.currentTimeMillis();
        switch (SearchTestByArgs.Mode) {
            case "trackSearch":
                SearchTestByArgs.TrackSearchTest();
                break;
            case "trackPagedSearch":
                SearchTestByArgs.TrackPagedSearchTest();
                break;
            case "posNonSpacialSearch":
                SearchTestByArgs.PosNonSpacialSearchTest();
                break;
            case "posSpacialSearch":
                SearchTestByArgs.PosSpacialSearchTest();
                break;
            default:
                System.out.println("Invalid command!");
        }
        long end = System.currentTimeMillis();
        long useTime = end - start;
        System.out.println("Overall Response time: " + useTime + "ms");

    }

    void TrackSearchTest () {
        for (int time = 0; time < TimeRange; time += TimeRange/5) {
            long start = System.currentTimeMillis();
            long startTime = System.currentTimeMillis() - time;
            long endTime = System.currentTimeMillis();
            String businessParams = "{\"city\":\"" + City + "\",\"devbtype\":" + Devbtype + ",\"devid\":\"" + Devid + "\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + "}";
            System.out.println(businessParams);
            TrackSearchWs trackSearchWs = new TrackSearchWs();
            String queryResult = trackSearchWs.services(null, businessParams);
            long end = System.currentTimeMillis();
            long useTime = end - start;
//            System.out.println(queryResult);
            System.out.println("Response time: " + useTime + "ms");
        }
    }

    void TrackPagedSearchTest() {
        long startTime = System.currentTimeMillis() - TimeRange;
        long endTime = System.currentTimeMillis();
        String businessParamsPaged = "{\"city\":\"" + City + "\",\"devbtype\":" + Devbtype + ",\"devid\":\"" + Devid + "\",\"startTime\":"
                + startTime + ",\"endTime\":" + endTime + ",\"page\":" + Page + ",\"rows\":" + Row + "}";
        TrackPagedSearchWs trackPagedSearchWs = new TrackPagedSearchWs();
        String queryResultPaged = trackPagedSearchWs.services(null, businessParamsPaged);
    }

    void PosNonSpacialSearchTest() {
        PosNonSpacialSearchWs posNonSpacialSearchWs = new PosNonSpacialSearchWs();
        String result = posNonSpacialSearchWs.services(null, null);
    }

    void PosSpacialSearchTest() {
        PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
        long startTime = System.currentTimeMillis() - TimeRange;
        long endTime = System.currentTimeMillis();
        String result = null;
        switch (Shape) {
            case "rectangle": {
                for (int i = 0; i < 10; i++){
                    double leftTop_x = x1;
                    double leftTop_y = y1;
                    double rightBottom_x = x2;
                    double rightBottom_y = y2;
                    Rectangle rectangle = new Rectangle(new Point(leftTop_x, leftTop_y), new Point(rightBottom_x, rightBottom_y));
                    double percentage = Double.parseDouble(Percent);
                    if (percentage >= 0 && percentage < 1) {
                        rectangle = zoomInRectangle(percentage);
                    }
                    LeftTop = rectangle.getLeftTopX() + "," + rectangle.getLeftTopY();
                    RightBottom = rectangle.getRightBottomX() + "," + rectangle.getRightBottomY();
                    String searchRectangle = "{\"type\":\"rectangle\",\"leftTop\":\"" + LeftTop + "\",\"rightBottom\":\"" + RightBottom
                            + "\",\"geoStr\":null,\"longitude\":null,\"latitude\":null,\"radius\":null,\"startTime\":" + startTime +
                            ",\"endTime\":" + endTime + "}";
                    long start = System.currentTimeMillis();
                    System.out.println(String.format("%f-%f, %f-%f", leftTop_x, rightBottom_x, leftTop_y, rightBottom_y));
                    posSpacialSearchWs.service(null, searchRectangle, startTime, endTime, id);
                    long end = System.currentTimeMillis();
                    long useTime = end - start;
                    System.out.println("Response time: " + useTime + "ms");
                }
            }break;
            case "circle": {
                for (int i = 0; i < 10; i ++) {
                    double longitude = Double.parseDouble(Longitude);
                    double latitude = Double.parseDouble(Latitude);
                    double radius = Double.parseDouble(Radius) + i;
                    double percentage = Double.parseDouble(Percent);
                    if (percentage >= 0 && percentage < 1) {
                        Rectangle rectangle = zoomInRectangle(percentage);
                        double xLen = rectangle.getRightBottomX() - rectangle.getLeftTopX();
                        double yLen = rectangle.getLeftTopY() - rectangle.getRightBottomY();
                        double len = xLen < yLen ? xLen : yLen;
                        radius = len / 2;
                        longitude = rectangle.getLeftTopX() + radius;
                        latitude = rectangle.getLeftTopY() - radius;
                    }
                    String searchCircle = "{\"type\":\"circle\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":null,\"longitude\":"
                            + longitude + ",\"latitude\":" + latitude + ",\"radius\":" + radius + ",\"startTime\":" + startTime +
                            ",\"endTime\":" + endTime + "}";
                    long start = System.currentTimeMillis();
                    posSpacialSearchWs.service(null, searchCircle, startTime, endTime, id);
                    long end = System.currentTimeMillis();
                    long useTime = end - start;
                    System.out.println("Response time: " + useTime + "ms");
                }
            }break;
            case "polygon": {
                long start = System.currentTimeMillis();
                String[] strings = Geostr.split(",");
                String geostr = "[";
                for (int i = 0; i < strings.length; i++) {
                    geostr += "\"" + strings[i] + " " + strings[++i] + "\"";
                    if (i < strings.length - 1) {
                        geostr += ",";
                    }
                }
                geostr += "]";
                System.out.println(geostr);
                String searchPolygon = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoSt" +
                        "r\":" + geostr + ",\"lon\":null,\"lat\":null,\"radius\":null,\"startTime\":" + startTime +
                        ",\"endTime\":" + endTime + "}";
                result = posSpacialSearchWs.service(null, searchPolygon, startTime, endTime, id);
                System.out.println(result);
                long end = System.currentTimeMillis();
                long useTime = end - start;
                System.out.println("Response time: " + useTime + "ms");
            }break;
            default: System.out.println("Invalid command!");
        }

    }

    Rectangle GetPercentRect () {
        double percent = Double.parseDouble(Percent);
        double xLen = (x2 - x1) * (1 - percent);
        double yLen = (y1 - y2) * (1 - percent);
        double S = (x2 - x1) * (y1 - y2);
        while (true) {
            double random = Math.random();
            leftTop_x = x1 + random * xLen;
            leftTop_y = y1 - random * yLen;
            rightBottom_x = leftTop_x + random * (x2 - x1) * 4;

            if (rightBottom_x <= x2) {
                rightBottom_y = leftTop_y - S * percent / (rightBottom_x - leftTop_x);
                if (rightBottom_y >= y2) {
                    break;
                } else {
                    continue;
                }
            } else {
                continue;
            }
        }
        return new Rectangle(new Point(leftTop_x, leftTop_y), new Point(rightBottom_x, rightBottom_y));
    }

    Rectangle zoomInRectangle(double percentage) {
        final double oneDimensionalPercentage = Math.sqrt(percentage);
        double xLen = Math.abs(x2 - x1) * oneDimensionalPercentage;
        double yLen = Math.abs(y1 - y2) * oneDimensionalPercentage;
        Random random = new Random();

        double lefttop_x = Math.min(x1, x2) + random.nextDouble() * Math.abs(x2 - x1) * (1 - oneDimensionalPercentage);
        double lefttop_y = Math.min(y1, y2) + random.nextDouble() * Math.abs(y1 - y2) * (1 - oneDimensionalPercentage);
        double rightbottm_x = lefttop_x + xLen;
        double rightbotton_y = lefttop_y + yLen;
        System.out.println(String.format("%f -> %f ======== %f -> %f", y1, y2, lefttop_y, rightbotton_y));
        return new Rectangle(new Point(lefttop_x, lefttop_y), new Point(rightbottm_x, rightbotton_y));
    }
}
