package indexingTopology.util.track;

import java.util.Scanner;

/**
 * Created by billlin on 2017/12/17
 */
public class searchTest {
    public static void main(String[] args) {
        String permissionParams = null;
//        String businessParams = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"asd\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + "}";
//        String businessParamsPaged = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"asd\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + ",\"page\":2,\"rows\":10}";
//        TrackSearchWs trackSearchWs = new TrackSearchWs();
//        String queryResult = trackSearchWs.services(permissionParams, businessParams);
//        System.out.println(queryResult);
//        TrackPagedSearchWs trackPagedSearchWs = new TrackPagedSearchWs();
//        String queryResultPaged = trackPagedSearchWs.services(permissionParams, businessParamsPaged);
//        System.out.println(queryResultPaged);
        Scanner scanner =new Scanner(System.in);
        while (scanner.hasNext()){
            String  querySelect = scanner.next();
            switch (querySelect){
                case "1" : {
                    long startTime = System.currentTimeMillis() - 1000 * 1000;
                    long endTime = System.currentTimeMillis();
                    String businessParams = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"0x0101\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + "}";
                    TrackSearchWs trackSearchWs = new TrackSearchWs();
                    String queryResult = trackSearchWs.services(permissionParams, businessParams);
                    System.out.println(queryResult);
                    break;
                }
                case "2" : {
                    long startTime = System.currentTimeMillis() - 1000 * 1000;
                    long endTime = System.currentTimeMillis();
                    String businessParamsPaged = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"0x0101\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + ",\"page\":2,\"rows\":10}";
                    TrackPagedSearchWs trackPagedSearchWs = new TrackPagedSearchWs();
                    String queryResultPaged = trackPagedSearchWs.services(permissionParams, businessParamsPaged);
                    System.out.println(queryResultPaged);
                    break;
                }
                case "3" : {
                    PosNonSpacialSearchWs posNonSpacialSearchWs = new PosNonSpacialSearchWs();
                    String result = posNonSpacialSearchWs.services(null, null);
                    System.out.println(result);
                    break;
                }
                case "4" : {
                    String searcRectangle = "{\"type\":\"rectangle\",\"leftTop\":\"80,100\",\"rightBottom\":\"90,80\",\"geoStr\":null,\"longitude\":null,\"latitude\":null,\"radius\":null}";
                    PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
                    String result = posSpacialSearchWs.service(null, searcRectangle);
                    System.out.println(result);
                    break;
                }
                case "5" : {
                    String searchCircle = "{\"type\":\"circle\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":null,\"longitude\":80,\"latitude\":75,\"radius\":10}";
                    PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
                    String result = posSpacialSearchWs.service(null, searchCircle);
                    System.out.println(result);
                    break;
                }
                case "6" : {
//                    String searchPolygon = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":[\"70 70\",\"85 80\",\"90 75\",\"85 70\",\"70 70\"],\"lon\":null,\"lat\":null,\"radius\":null}";
                    String searchPolygon = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoSt" +
                            "r\":[\"80 80\",\"90 80\",\"90 100\",\"80 100\"],\"lon\":null,\"lat\":null,\"radius\":null}";
                    PosSpacialSearchWs posSpacialSearchWs = new PosSpacialSearchWs();
                    String result = posSpacialSearchWs.service(null, searchPolygon);
                    System.out.println(result);
                    break;
                }
                default : break;
            }
        }
    }
}
