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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
        DataSchema schema = getDataSchema();
        double selectivityOnOneDimension = Math.sqrt(Selectivity);
        Random random = new Random();
        double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
        double y = y1 + (y2 - y1) * (1 - selectivityOnOneDimension) * random.nextDouble();

        final double xLow = x;
        final double xHigh = x + selectivityOnOneDimension * (x2 - x1);
        final double yLow = y;
        final double yHigh = y + selectivityOnOneDimension * (y2 - y1);
        JSONObject queryResponse = new JSONObject();
        JSONArray queryResult = new JSONArray();
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, 10001);
        try {
            queryClient.connectWithTimeout(10000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long timeMillis = System.currentTimeMillis();
        long timeMillis2 = System.currentTimeMillis() - 600 * 1000;
        GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, Double.MAX_VALUE,
                timeMillis2,
                timeMillis, null, null,null, null, null);
        Date dateOld = new Date(timeMillis); // 根据long类型的毫秒数生命一个date类型的时间
        String sDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateOld); // 把date类型的时间转换为string
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null; // 把String类型转换为Date类型
        try {
            date = formatter.parse(sDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);


        Date dateOld2 = new Date(timeMillis2); // 根据long类型的毫秒数生命一个date类型的时间
        String sDateTime2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateOld2); // 把date类型的时间转换为string
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date2 = null; // 把String类型转换为Date类型
        try {
            date2 = formatter2.parse(sDateTime2);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String currentTime2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date2);

        System.out.println(currentTime2 + "  " + currentTime);
        try {
            QueryResponse response = queryClient.query(queryRequest);
            List<DataTuple> tuples = response.getTuples();
            for (DataTuple tuple : tuples) {
                queryResult.add(schema.getJsonFromDataTupleWithoutZcode(tuple));
                System.out.println(schema.getJsonFromDataTupleWithoutZcode(tuple));
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
        return result;
    }


    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();
        schema.addIntField("devbtype");
        schema.addVarcharField("devstype", 32);
        schema.addVarcharField("devid", 32);
        schema.addVarcharField("city", 32);
        schema.addDoubleField("longitude");
        schema.addDoubleField("latitude");
        schema.addDoubleField("altitude");
        schema.addDoubleField("speed");
        schema.addDoubleField("direction");
        schema.addLongField("locationtime");
        schema.addIntField("workstate");
        schema.addVarcharField("clzl", 32);
        schema.addVarcharField("hphm", 32);
        schema.addIntField("jzlx");
        schema.addVarcharField("jybh", 32);
        schema.addVarcharField("jymc", 32);
        schema.addVarcharField("lxdh", 32);
        schema.addVarcharField("ssdwdm", 32);
        schema.addVarcharField("ssdwmc", 32);
        schema.addVarcharField("teamno", 32);
        schema.addVarcharField("dth", 32);
        schema.addVarcharField("reserve1", 32);
        schema.addVarcharField("reserve2", 32);
        schema.addVarcharField("reserve3", 32);
        schema.setTemporalField("locationtime");
        schema.addIntField("zcode");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

    public static void main(String[] args) {
        PosNonSpacialSearchWs posNonSpacialSearchWs = new PosNonSpacialSearchWs();
        String result = posNonSpacialSearchWs.services(null, null);
        System.out.println(result);
    }
}
