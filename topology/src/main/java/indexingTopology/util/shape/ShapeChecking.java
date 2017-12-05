package indexingTopology.util.shape;

import net.sf.json.JSON;
import net.sf.json.JSONObject;
import org.apache.kafka.common.protocol.types.Field;

import java.util.ArrayList;
import java.util.Random;

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
            if(!jsonObject.get("geoStr").equals(null) || !jsonObject.get("lon").equals(null) || !jsonObject.get("lat").equals(null) || !jsonObject.get("radius").equals(null)){
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
        JSONObject jsonObject = JSONObject.fromObject(searchTest);
        ShapeChecking shapeChecking = new ShapeChecking(jsonObject);
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
        JSONObject jsonObject3 = JSONObject.fromObject(searchTest3);
        System.out.println(jsonObject3.get("geoStr"));
        String geo = String.valueOf(jsonObject3.get("geoStr"));
        String geoClean = shapeChecking.deleteCharString(geo,'[',']','\"');
        System.out.println(geoClean);
        ShapeChecking shapeChecking3 = new ShapeChecking(jsonObject3);
                ArrayList arrayList3 = shapeChecking3.split();
        if(shapeChecking3.getError() == null) {
            for(int i=0;i<arrayList3.size();i++){
                System.out.print(Integer.valueOf(arrayList3.get(i).toString()) + " ");
            }
        }
    }
}
