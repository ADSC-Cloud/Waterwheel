package indexingTopology.util.shape;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import info.batey.kafka.unit.KafkaUnit;
import kafka.producer.KeyedMessage;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by billlin on 2017/12/4
 */
public class ShapeChecking {
    JSONObject jsonObject;
    String error;
    ArrayList arrayList;

    public ShapeChecking(JSONObject jsonObject){
        this.jsonObject = jsonObject;
        this.error = null;
         arrayList = new ArrayList();
    }

    public ArrayList split(){
        String type = String.valueOf(jsonObject.get("type"));
        if(type.equals("rectangle")){
            if(jsonObject.get("geoStr") == null /*|| !jsonObject.get("lon").equals(null) || !jsonObject.get("lat").equals(null) || !jsonObject.get("radius").equals(null)*/){
                error = "102";
                return null;
            }
            String left = String.valueOf(jsonObject.get("leftTop"));
            String right = String.valueOf(jsonObject.get("rightBottom"));
            for(int i = 0;i<left.split(",").length;i++){
                arrayList.add(left.split(",")[i]);
            }
            for(int i = 0;i<right.split(",").length;i++){
                arrayList.add(right.split(",")[i]);
            }
            if(left.split(",").length != 2 || right.split(",").length != 2){
                System.out.println(left.split(",").length);
                System.out.println(right.split(",").length);
                error = "1002";
                return null;
            }
            return arrayList;
        }
        else if(type.equals("polygon")){
//            JSONObject geoStr = JSONObject.fromObject(jsonObject.get("geoStr"));
//            if(!jsonObject.get("geoStr").equals(null) || jsonObject.get("rightBottom") != null || jsonObject.get("longitude") != null || jsonObject.get("latitude") != null || jsonObject.get("radius") != null){
//                error = "1002";
//                return null;
//            }
            String geo = String.valueOf(jsonObject.get("geoStr"));
            String geoClean = deleteCharString(geo,'[',']','\"');
            String[] geoToPoint = geoClean.split(",");
//            System.out.println(geoToPoint[0].split("  ")[1]);
            for(int i = 0;i < geoToPoint.length;i++){
                String[] splited = geoToPoint[i].split("  ");
                for(int j = 0;j < 2;j++){
                    arrayList.add(splited[j]);
                }
            }
            return arrayList;
        }
        else if(type.equals("circle")){
//            if(!jsonObject.get("geoStr").equals(null) || jsonObject.get("rightBottom") != null || jsonObject.get("geoStr") != null){
//                error = "1002";
//                return null;
//            }
            arrayList.add(jsonObject.get("lon"));
            arrayList.add(jsonObject.get("lat"));
            arrayList.add(jsonObject.get("radius"));
            return arrayList;
        }
        else {
            error = "1101";
            return null;
        }
    }

    public String getError(){
        return this.error;
    }

    public String deleteCharString(String sourceString, char chElemData1, char chElemData2, char chElemData3) {

        String deleteString = "";

        for (int i = 0; i < sourceString.length(); i++) {
            if (sourceString.charAt(i) != chElemData1) {
                if(sourceString.charAt(i) != chElemData2){
                    if(sourceString.charAt(i) != chElemData3) {
                        deleteString += sourceString.charAt(i);
                    }
                }
            }
        }
        return deleteString;

    }


    // 过滤特殊字符
    public static String StringFilter(String str) throws PatternSyntaxException {
        // 只允许字母和数字 // String regEx = "[^a-zA-Z0-9]";
        // 清除掉所有特殊字符
        String regEx = "[`~!@#$%^&*()+=|{}';'\\[\\]<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }


    public static void main(String[] args) {
        Random random = new Random();
         double Selectivity = 1;
        double selectivityOnOneDimension = Math.sqrt(Selectivity);
         final double x1 = 40.012928;
         final double x2 = 40.023983;
        double x = x1 + (x2 - x1) * (1 - selectivityOnOneDimension) * random.nextDouble();
        final double xLow = x;
        final double xHigh = x + selectivityOnOneDimension * (x2 - x1);
        System.out.println(xLow);
        System.out.println(xHigh);
        System.out.println(String.valueOf(Math.random()));
        String searchTest = "{\"type\":\"rectangle\",\"leftTop\":\"113,24\",\"rightBottom\":\"112,23\",\"geoStr\":null,\"lon\":null,\"lat\":null,\"radius\":null}";
        String searchTest3 = "{\"type\":\"polygon\",\"leftTop\":null,\"rightBottom\":null,\"geoStr\":[\"80  75\",\"85  80\",\"90  75\",\"85  70\"],\"lon\":null,\"lat\":null,\"radius\":null}";
        JSONObject jsonObject = JSONObject.parseObject(searchTest);
        ShapeChecking shapeChecking = new ShapeChecking(jsonObject);
        System.out.println(jsonObject);
        ArrayList arrayList = shapeChecking.split();
        if(shapeChecking.getError() == null) {
            for(int i=0;i<arrayList.size();i++){
                System.out.println(Integer.valueOf(arrayList.get(i).toString().trim()));
            }
        }
        else {
            System.out.printf("ErrorCode : " + shapeChecking.getError());
        }
        System.out.println(Math.random());
        System.out.println(Math.random());
        System.out.println((int) (Math.random() * 100));
        JSONObject jsonObject3 = JSONObject.parseObject(searchTest3);
        System.out.println(jsonObject3.get("geoStr"));
        String geo = String.valueOf(jsonObject3.get("geoStr"));
        String geoClean = shapeChecking.deleteCharString(geo,'[',']','\"');
        System.out.println(geoClean);
        ShapeChecking shapeChecking3 = new ShapeChecking(jsonObject3);
                ArrayList arrayList3 = shapeChecking3.split();
        if(shapeChecking3.getError() == null) {
            for(int i=0;i<arrayList3.size();i++){
                System.out.print(Integer.valueOf(arrayList3.get(i).toString()) + "  ");
            }
        }
        Object object = new Object();
        String searchTest11 = "{\"type\":\"rectangle\",\"leftTop\":\"113,24\",\"rightBottom\":\"112,23\",\"geoStr\":null,\"lon\":null,\"lat\":null,\"radius\":null}";
        JSONObject jsonObject1 = JSONObject.parseObject(searchTest11);
//        try{
//            Double jsonInteger = jsonObject.getDouble("type");
//            System.out.println(jsonInteger);
//        }catch (JSONException e){
//            System.out.println("Error !");;
//        }
        object = (Object)jsonObject1;
        System.out.println(object);

        DataSchema schema = new DataSchema();
        schema.addDoubleField("lon");
        schema.addDoubleField("lat");
        schema.addIntField("devbtype");
        schema.addVarcharField("devid", 8);
        schema.addVarcharField("id", 32);
        schema.addIntField("zcode");
        schema.addLongField("timestamp");
        schema.setTemporalField("timestamp");



        int len = schema.getNumberOfFields();
        DataTuple dataTuple = new DataTuple();
        String objectStr = "";
        Object attribute = new Object();
        attribute = "1";
        dataTuple.add(attribute);
        attribute = 2;
        dataTuple.add(attribute);
        attribute = "3";
        dataTuple.add(attribute);
        attribute = "4";
        dataTuple.add(attribute);
        attribute = "5";
        dataTuple.add(attribute);
        attribute = "6";
        dataTuple.add(attribute);
        attribute = 11.7;
        dataTuple.add(attribute);
        JSONObject objectfromTuple= schema.getJsonFromDataTuple(dataTuple);
        System.out.println(objectfromTuple);

        ArrayList<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        System.out.println(list.toString());
        String str = "*adCVs*34_a _09_b5*[/435^*&城池()^$$&*).{}+.|.)%%*(*.中国}34{45[]12.fd'*&999下面是中文的字符￥……{}【】。，；’“‘”？";
        String s = "<script>alert(1).</script>";
        String ll = "[10.21.25.203:9092, 10.21.25.204:9092, 10.21.25.205:9092]";
        System.out.println(ll);
        System.out.println(StringFilter(ll));
    }
}
