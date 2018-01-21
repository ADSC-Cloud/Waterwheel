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
import java.util.List;

/**
 * Created by billlin on 2017/12/15
 */
public class TrackPagedSearchWs implements Serializable{
    private String city;
    private int devbtype;
    private String devid;
    private long startTime;
    private long endTime;
    private int page;
    private int rows;
    private String errorCode;
    private String errorMsg;
    private String hdfsIP = "68.28.8.91";
    private String QueryServerIp = "localhost";

    public TrackPagedSearchWs(){

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
            }
            catch (JSONException e){
                queryResponse.put("result", null);
                queryResponse.put("errorCode", "1002");
                queryResponse.put("errorMsg", "参数值无效或者缺失必填参数");
                String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
                return result;
            }
            catch (NullPointerException e){
                queryResponse.put("result", null);
                queryResponse.put("errorCode", "1002");
                queryResponse.put("errorMsg", "参数值无效或者缺失必填参数");
                String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
                return result;
            }
            catch (IllegalArgumentException e){
                queryResponse.put("result", null);
                queryResponse.put("errorCode", "1");
                queryResponse.put("errorMsg", "参数值无效或者缺失必填参数");
                String result = JSONObject.toJSONString(queryResponse, SerializerFeature.WriteMapNullValue);
                return result;
            }
        }catch (JSONException e){// query failed, json value invalid
            errorCode = "1001";
//            errorMsg = Error(errorCode);
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

            int totalPage = tuples.size()/rows;

            queryResponse.put("success", true);
            JSONArray queryResult = new JSONArray();
            if(tuples.size() > 0 && tuples.size() > rows * (page - 1)){
                for (int i = rows * (page - 1); i < rows * page; i++) {
                    if(i >= tuples.size()){
                        break;
                    }
                    JSONObject jsonFromTuple = schema.getJsonFromDataTupleWithoutZcode(tuples.get(i));
                    queryResult.add(jsonFromTuple);
                    System.out.println(jsonFromTuple);
                }
            }
            JSONObject result = new JSONObject();
            result.put("total", tuples.size());
            result.put("page",page);
            result.put("sortName",null);
            result.put("sortOrder",null);
            result.put("city",city);
            result.put("devbtype",devbtype);
            result.put("devid",devid);
            result.put("startTime",startTime);
            result.put("endTime",endTime);
            result.put("startRowKey",null);
            result.put("stopRowKey",null);
            result.put("rows",queryResult);
            queryResponse.put("result", result);
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

    public boolean getQueryJson(JSONObject businessParams) throws JSONException, NullPointerException{
//        try {
            this.city = (String)businessParams.get("city");
            this.devbtype = (int)businessParams.get("devbtype");
            this.devid = (String)businessParams.get("devid");
            this.startTime = (long)businessParams.get("startTime");
            this.endTime = (long)businessParams.get("endTime");
            this.page = (int)businessParams.get("page");
            this.rows = (int)businessParams.get("rows");
            if(city == null || devid == null || businessParams.size() > 7){
                throw new IllegalArgumentException();
            }
        return true;
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
//        schema.setTemporalField("locationtime");
////        schema.addLongField("timestamp");
//        schema.addIntField("zcode");
//        schema.setPrimaryIndexField("zcode");
//
//        return schema;
//    }

}
