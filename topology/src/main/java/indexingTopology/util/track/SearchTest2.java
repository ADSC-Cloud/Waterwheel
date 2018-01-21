package indexingTopology.util.track;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created by zelin on 2017/1/11
 */
public class SearchTest2 {
    @Option(name = "--mode", aliases = {"-m"}, usage = "trackSearch|trackPagedSearch|posNonSpacialSearch|posSpacialSearch")
    private String Mode = "Not Given";

    @Option(name = "--time-range", aliases = {"-time"}, usage = "the search time range")
    private long TimeRange = 1000 * 1000;

    @Option(name = "--shape", aliases = {"-s"}, usage = "rectangle|circle|polygon")
    private String Shape = "Not Given";

    @Option(name = "--longitude", aliases = {"-lo"}, usage = "longitude of circle")
    private String Longitude = "Not Given";

    @Option(name = "--latitude", aliases = {"-la"}, usage = "latitude of circle")
    private String Latitude = "Not Given";

    @Option(name = "--radius", usage = "radius of circle")
    private String Radius = "Not Given";

    @Option(name = "--lefttop", usage = "lefttop of rectangle")
    private String LeftTop = "Not Given";

    @Option(name = "--rightbottom", usage = "rightbottom of rectangle")
    private String RightBottom = "Not Given";

    @Option(name = "--geostr", usage = "geostr of polygon")
    private String Geostr = "Not Given";

    @Option(name = "--page", usage = "page of page search")
    private String Page = "1";

    @Option(name = "--row", usage = "row of page search")
    private String Row = "10";

    public static void main(String[] args) {

        SearchTest2 SearchTest2 = new SearchTest2();

        CmdLineParser parser = new CmdLineParser(SearchTest2);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            parser.printUsage(System.out);
        }

        long start = System.currentTimeMillis();
        switch (SearchTest2.Mode) {
            case "trackSearch":
                SearchTest2.TrackSearchTest();
                break;
            case "trackPagedSearch":
                SearchTest2.TrackPagedSearchTest();
                break;
            case "posNonSpacialSearch":
                SearchTest2.PosNonSpacialSearchTest();
                break;
            case "posSpacialSearch":
                SearchTest2.PosSpacialSearchTest();
                break;
            default:
                System.out.println("Invalid command!");
        }
        long end = System.currentTimeMillis();
        long useTime = end - start;
        System.out.println("总耗时： " + useTime + "ms");

    }

    void TrackSearchTest () {
        for (int time = 0; time < TimeRange; time += TimeRange/5) {
            long start = System.currentTimeMillis();
            long startTime = System.currentTimeMillis() - TimeRange;
            long endTime = System.currentTimeMillis();
            String businessParams = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"0x0101\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + "}";
            TrackSearchWs trackSearchWs = new TrackSearchWs();
            String queryResult = trackSearchWs.services(null, businessParams);
            long end = System.currentTimeMillis();
            long useTime = endTime - startTime;
            System.out.println("耗时： " + useTime + "ms");
        }
    }

    void TrackPagedSearchTest() {
        long startTime = System.currentTimeMillis() - TimeRange;
        long endTime = System.currentTimeMillis();
        String businessParamsPaged = "{\"city\":\"4406\",\"devbtype\":11,\"devid\":\"75736331\",\"startTime\":"
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
        String result = null;
        switch (Shape) {
            case "rectangle": {
                for (int i = 0; i < 100; i += 20){
                    long start = System.currentTimeMillis();
                    double leftTop_1 = Double.parseDouble(LeftTop.split(",")[0]) - i;
                    double leftTop_2 = Double.parseDouble(LeftTop.split(",")[1]) + i;
                    double rightBottom_1 = Double.parseDouble(RightBottom.split(",")[0]) + i;
                    double rightBottom_2 = Double.parseDouble(RightBottom.split(",")[1]) - i;
                    LeftTop = leftTop_1 + "," + leftTop_2;
                    RightBottom = rightBottom_1 + "," + rightBottom_2;
                    String searchRectangle = "{\"type\":\"rectangle\",\"leftTop\":\"" + LeftTop + "\",\"rightBottom\":\"" + RightBottom
                            + "\",\"geoStr\":null,\"longitude\":null,\"latitude\":null,\"radius\":null}";
                    posSpacialSearchWs.service(null, searchRectangle);
                    long end = System.currentTimeMillis();
                    long useTime = end - start;
                    System.out.println("耗时： " + useTime + "ms");
                }
            }break;
            case "circle": {
                for (int i = 0; i < 100; i++) {
                    long start = System.currentTimeMillis();
                    double radius = Double.parseDouble(Radius) + i;
                    String searchCircle = "{\"type\":\"circle\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":null,\"longitude\":"
                            + Longitude + ",\"latitude\":" + Latitude + ",\"radius\":" + Radius + "}";
                    posSpacialSearchWs.service(null, searchCircle);
                    long end = System.currentTimeMillis();
                    long useTime = end - start;
                    System.out.println("耗时： " + useTime + "ms");
                }
            }break;
            case "polygon": {
                long start = System.currentTimeMillis();
                String searchPolygon = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoSt" +
                        "r\":" + Geostr + ",\"lon\":null,\"lat\":null,\"radius\":null}";
                posSpacialSearchWs.service(null, searchPolygon);
                long end = System.currentTimeMillis();
                long useTime = end - start;
                System.out.println("耗时： " + useTime + "ms");
            }break;
            default: System.out.println("Invalid command!");
        }

    }
}
