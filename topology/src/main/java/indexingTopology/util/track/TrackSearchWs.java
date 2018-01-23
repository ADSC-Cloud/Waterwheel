package indexingTopology.util.track;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import indexingTopology.api.client.GeoTemporalQueryClient;
import indexingTopology.api.client.GeoTemporalQueryRequest;
import indexingTopology.api.client.QueryResponse;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTuplePredicate;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * Created by billlin on 2017/12/15
 */
public class TrackSearchWs implements Serializable{

    private int devbtype;
    private String devstype;
    private String devid;
    private String city;
    private double longitude;
    private double latidute;
    private double altitude;
    private double speed;
    private double direction;
    private long locationtime;
    private int workstate;
    private String clzl;
    private String hphm;
    private int jzlx;
    private String jybh;
    private String jymc;
    private String lxdh;
    private String ssdwdm;
    private String ssdwmc;
    private String teamno;
    private String dth;
    private String reserve1;
    private String reserve2;
    private String reserve3;

    private long startTime;
    private long endTime;
    private String errorCode;
    private String errorMsg;
    private String hdfsIP = "68.28.8.91";
    private String QueryServerIp = "68.28.8.91";

    public TrackSearchWs(){

//        this.city = (String)businessParams.get("city");
//        this.devbtype = (int)businessParams.get("devbtype");
//        this.devid = (String)businessParams.get("devid");
//        this.startTime = (long)businessParams.get("startTime");
//        this.endTime = (long)businessParams.get("endTime");
    }

    public String services(String permissionParams, String businessParams) {
        JSONObject queryResponse = new JSONObject();
        try{
            JSONObject jsonObject = JSONObject.parseObject(businessParams);
            try{
                getQueryJson(jsonObject); // query failed,json format is error
            }catch (JSONException e){
                queryResponse.put("result", null);
                queryResponse.put("errorCode", "1002");
                queryResponse.put("errorMsg", "参数值无效或者缺失必填参数");
                String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
                return result;
            }catch (NullPointerException e){
                queryResponse.put("result", null);
                queryResponse.put("errorCode", "1002");
                queryResponse.put("errorMsg", "参数值无效或者缺失必填参数");
                String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
                return result;
            }catch (IllegalArgumentException e){
                queryResponse.put("result", null);
                queryResponse.put("errorCode", "1002");
                queryResponse.put("errorMsg", "参数值无效或者缺失必填参数");
                String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
                return result;
            }
        }catch (JSONException e){// query failed, json value invalid
            errorCode = "1001";
            queryResponse.put("result", null);
            queryResponse.put("errorCode", errorCode);
            queryResponse.put("errorMsg", "参数解析失败，参数格式存在问题");
            String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
            return result;
        }
        // query success
        GeoTemporalQueryClient queryClient = new GeoTemporalQueryClient(QueryServerIp, 10001);
        try {
            queryClient.connectWithTimeout(10000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        DataSchema schema = getDataSchema();
        DataTuplePredicate predicate;
        predicate = t -> CheckEqual(schema.getValue("city", t),schema.getValue("devbtype", t),schema.getValue("devid", t));
        GeoTemporalQueryRequest queryRequest = new GeoTemporalQueryRequest<>(Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE, Double.MAX_VALUE,
                startTime,
                endTime, predicate, null, null, null, null);
        try {
            QueryResponse response = queryClient.query(queryRequest);
            DataSchema outputSchema = response.getSchema();
            System.out.println("datatuples : " + response.dataTuples.size());
            List<DataTuple> tuples = response.getTuples();

            queryResponse.put("success", true);
            JSONArray queryResult = new JSONArray();
            for (int i = 0; i < tuples.size(); i++) {
                JSONObject jsonFromTuple = schema.getJsonFromDataTupleWithoutZcode(tuples.get(i));
                queryResult.add(jsonFromTuple);
                System.out.println(jsonFromTuple);
            }
            queryResponse.put("result", queryResult);
            queryResponse.put("errorCode", null);
            queryResponse.put("errorMsg", null);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            queryClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
        return result;
    }

    public void getQueryJson(JSONObject businessParams) throws JSONException, NullPointerException{
        this.devbtype = (int)businessParams.get("devbtype");
        this.devid = (String)businessParams.get("devid");
        this.city = (String)businessParams.get("city");
        this.startTime = (long)businessParams.get("startTime");
        this.endTime = (long)businessParams.get("endTime");
        if(city == null ||  devid == null){
            throw new IllegalArgumentException("Missing required parameters");
        }
    }

//    public boolean CheckEqual(String city, int devbtype, String devid) {
//        if (this.city.equals(city) && this.devbtype == devbtype && this.devid.equals(devid)) {
//            return true;
//        }
//        else
//            return false;
//    }


    public boolean CheckEqual(Object city, Object devbtype, Object devid) {
        if(city == null || devbtype == null ||devid == null ){
            return false;
        }
        String cityStr = (String) city;
        int devbtypeInt = (int) devbtype;
        String devidStr = (String) devid;
        if (this.city.equals(cityStr) && this.devbtype == devbtypeInt && this.devid.equals(devidStr)) {
            return true;
        }
        else
            return false;
    }

    static private DataSchema getDataSchema() {
        DataSchema schema = new DataSchema();

        schema.addIntField("devbtype");
        schema.addVarcharField("devstype", 64);
        schema.addVarcharField("devid", 64);
        schema.addVarcharField("city", 64);
        schema.addDoubleField("longitude");
        schema.addDoubleField("latitude");
        schema.addDoubleField("altitude");
        schema.addDoubleField("speed");
        schema.addDoubleField("direction");
        schema.addLongField("locationtime");
        schema.addIntField("workstate");
        schema.addVarcharField("clzl", 64);
        schema.addVarcharField("hphm", 64);
        schema.addIntField("jzlx");
        schema.addVarcharField("jybh", 64);
        schema.addVarcharField("jymc", 64);
        schema.addVarcharField("lxdh", 64);
        schema.addVarcharField("ssdwdm", 64);
        schema.addVarcharField("ssdwmc", 64);
        schema.addVarcharField("teamno", 64);
        schema.addVarcharField("dth", 64);
        schema.addVarcharField("reserve1", 64);
        schema.addVarcharField("reserve2", 64);
        schema.addVarcharField("reserve3", 64);
        schema.setTemporalField("locationtime");
        schema.addIntField("zcode");
        schema.setPrimaryIndexField("zcode");
        return schema;
    }

//    static private DataSchema getDataSchema() {
//        DataSchema schema = new DataSchema();
//        schema.addDoubleField("lon");
//        schema.addDoubleField("lat");
//        schema.addIntField("devbtype");
//        schema.addVarcharField("devid", 8);
////        schema.addVarcharField("id", 64);
//        schema.addVarcharField("city",64);
//        schema.addLongField("locationtime");
////        schema.addLongField("timestamp");
//        schema.addIntField("zcode");
//        schema.setPrimaryIndexField("zcode");
//
//        return schema;
//    }

}
