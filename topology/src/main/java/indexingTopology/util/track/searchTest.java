package indexingTopology.util.track;

/**
 * Created by billlin on 2017/12/17
 */
public class searchTest {
    public static void main(String[] args) {
        String permissionParams = null;
        long startTime = System.currentTimeMillis() - 10 * 1000;
        long endTime = System.currentTimeMillis();
        String businessParams = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"asd\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + "}";
        String businessParamsPaged = "{\"city\":\"4401\",\"devbtype\":2,\"devid\":\"asd\",\"startTime\":" + startTime + ",\"endTime\":" + endTime + ",\"page\":2,\"rows\":10}";
        TrackSearchWs trackSearchWs = new TrackSearchWs();
        String queryResult = trackSearchWs.services(permissionParams, businessParams);
        System.out.println(queryResult);
//        TrackPagedSearchWs trackPagedSearchWs = new TrackPagedSearchWs();
//        String queryResultPaged = trackPagedSearchWs.services(permissionParams, businessParamsPaged);
//        System.out.println(queryResultPaged);
    }
}
