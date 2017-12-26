package indexingTopology.util.track;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Create by zelin on 17-12-15
 **/
public class PosNonSpacialSearchWs {

    private String QueryServerIp = "localhost";
    static final double x1 = 40.012928;
    static final double x2 = 40.023983;
    static final double y1 = 116.292677;
    static final double y2 = 116.614865;

    private double Selectivity = 1;

    public String services(String permissionsParams, String businessParams) {
        DataSchema schema = new DataSchema();
        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        Random random = new Random();
        double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
        double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

        final double xLow = x;
        final double xHigh = x + selectivityOnOneDimension * (x2 - x1);
        final double yLow = y;
        final double yHigh = y + selectivityOnOneDimension * (y2 - y1);
        JSONObject queryResponse = new JSONObject();
        JSONArray queryResult = null;
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, 10001);
        try {
            queryClient.connectWithTimeout(10000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, Double.MAX_VALUE,
                System.currentTimeMillis() - 120 * 1000,
                System.currentTimeMillis(), null, null,null, null, null);
        try {
            QueryResponse response = queryClient.query(queryRequest);
            List<DataTuple> tuples = response.getTuples();
            for (DataTuple tuple : tuples) {
                queryResult.add(schema.getJsonFromDataTupleWithoutZcode(tuple));
                System.out.println(tuple);
            }

//            queryResponse.put("success", false);
//            queryResponse.put("result", null);
//            queryResponse.put("errorCode","1001");
//            queryResponse.put("errorMsg", "参数解析失败，参数格式存在问题");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        queryResponse.put("success", true);
        queryResponse.put("result", queryResult);
        queryResponse.put("errorCode", null);
        queryResponse.put("errorMsg", null);
        String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
        System.out.println(result);
        return result;
    }

    public static void main(String[] args) {
        PosNonSpacialSearchWs posNonSpacialSearchWs = new PosNonSpacialSearchWs();
        posNonSpacialSearchWs.services(null, null);
    }
}
