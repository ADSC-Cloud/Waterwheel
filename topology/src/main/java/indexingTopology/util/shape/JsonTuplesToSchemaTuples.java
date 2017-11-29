//package indexingTopology.util.shape;
//
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;
//import indexingTopology.common.data.DataSchema;
//import indexingTopology.common.data.DataTuple;
//import net.sf.json.JSONObject;
//
//
//import java.io.FileReader;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Create by zelin on 17-11-16
// **/
//public class JsonTuplesToSchemaTuples {
//
//    public static JSONObject schemaToJson(List<DataTuple> list, String type) {
//        if(type.equals("out")) {
//            TupleModel tupleModel = new TupleModel(list);
//            JSONObject jsonObject = JSONObject.fromObject(tupleModel);
//            return jsonObject;
//        }
//        return null;
//    }
//
//    public static Object jsonToSchema(DataSchema dataSchema, JsonObject object, String type) throws Exception{
//        if(type.equals("out")) {
//
//            System.out.println(object.toString());
//            List<DataTuple> dataTuples = dataSchema.getTuplesFromJson(object, "result");
//            TupleModel tupleModel = new TupleModel(dataTuples);
//            System.out.println(tupleModel.getResults().get(0).getDevbtype());
//            List<TupleModel> lists = new ArrayList<TupleModel>();
//            JSONObject jsonObject = JSONObject.fromObject(tupleModel);
//            System.out.println(jsonObject.toString());
//            return dataTuples;
//        }
//
//        if(type.equals("rectangle") || type.equals("circle")) {
//            System.out.println(object.toString());
//            int len = dataSchema.getNumberOfFields();
//            String str = "";
//            for (int i = 0; i < len; i++) {
//                String str1;
//                if (!object.get(dataSchema.getFieldName(i)).isJsonNull()) {
//                    str1 = object.get(dataSchema.getFieldName(i)).toString().replace("\"", "");
//                    if (i == 1 || i == 2) {
//                        str1 = str1.replace(",", "/");
//                    }
//                }
//                else {
//                    str1 = "null";
//                }
//                if(i < len - 1) {
//                    str = str + str1 + ",";
//                }
//                else {
//                    str = str + str1;
//                }
//            }
//            System.out.println(str);
//            DataTuple dataTuple = dataSchema.parseJsonTuple(str, ",");
//            return dataTuple;
//        }
//
//        if(type.equals("polygon")) {
//            System.out.println(object.toString());
//            int len = dataSchema.getNumberOfFields();
//            JsonArray geoStr = object.getAsJsonArray("geoStr");
////            System.out.println(geoStr.toString());
//            String str = new String();
//            for(int i = 0; i < len; i++) {
//                String str1 = new String();
//                if(!object.get(dataSchema.getFieldName(i)).isJsonNull()) {
//                    if(dataSchema.getFieldName(i).equals("geoStr")) {
//                        int readGeo = 0;
//                        for (JsonElement element : geoStr) {
//                            str1 = str1 + element.toString().replace("\"", "").replace(",", "/");
//                            if(readGeo < geoStr.size() - 1) {
//                                str1 = str1 + " ";
//                            }
//                            readGeo++;
//                        }
//                    }else {
//                        str1 = object.get(dataSchema.getFieldName(i)).toString().replace("\"", "");
//                    }
//                }
//                else {
//                    str1 = "null";
//                }
//                if(i < len - 1) {
//                    str = str + str1 + ",";
//                }else {
//                    str = str + str1;
//                }
//            }
//            System.out.println(str);
//            DataTuple dataTuple = dataSchema.parseJsonTuple(str, ",");
//            return dataTuple;
//        }
//
//
//        return null;
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        JsonTuplesToSchemaTuples jsonTuplesToSchemaTuples = new JsonTuplesToSchemaTuples();
//        DataSchema dataSchemaOut = jsonTuplesToSchemaTuples.initOut();
//        DataSchema dataSchemaRectangle = jsonTuplesToSchemaTuples.initRectangle();
//        JsonParser parser = new JsonParser();
//        JsonObject objectOut =  (JsonObject) parser.parse(new FileReader("topology/jsonfile/testjson.json"));
//        JsonObject objectRectangle = (JsonObject) parser.parse(new FileReader("topology/jsonfile/rectangle.json"));
//        JsonObject objectPolygon = (JsonObject) parser.parse(new FileReader("topology/jsonfile/polygon.json"));
//        JsonObject objectCircle = (JsonObject) parser.parse(new FileReader("topology/jsonfile/circul"));
//        List<DataTuple> list = (List<DataTuple>) jsonToSchema(dataSchemaOut, objectOut, "out");
////        DataTuple dataTupleRectangle = (DataTuple) jsonToSchema(dataSchemaRectangle, objectRectangle, "rectangle");
////        List<DataTuple> outList = JudgContain.checkInRect(list, dataTupleRectangle);
////        DataTuple dataTuplePolygon = (DataTuple) jsonToSchema(dataSchemaRectangle, objectPolygon, "polygon");
//        DataTuple dataTupleCircle = (DataTuple) jsonToSchema(dataSchemaRectangle, objectCircle, "circle");
////        List<DataTuple> outList = JudgContain.checkInPolygon(list, dataTuplePolygon);
//        List<DataTuple> outList = JudgContain.checkInCircle(list,dataTupleCircle);
//        JSONObject jsonObject = dataSchemaOut.getJsonFromDataTuple(list.get(0));
//        System.out.println("this is -----------------" + jsonObject);
//        System.out.println(outList.size());
//    }
//
//    public DataSchema initOut(){
//        DataSchema schema = new DataSchema();
//        schema.addIntField("devbtype");
//        schema.addVarcharField("devstype", 4);
//        schema.addVarcharField("devid", 6);
//        schema.addIntField("city");
//        schema.addDoubleField("longitude");
//        schema.addDoubleField("latitude");
//        schema.addDoubleField("speed");
//        schema.addDoubleField("direction");
//        schema.addLongField("locationtime");
//        schema.addIntField("workstate");
//        schema.addVarcharField("clzl",4);
//        schema.addVarcharField("hphm", 7);
//        schema.addIntField("jzlx");
//        schema.addLongField("jybh");
//        schema.addVarcharField("jymc", 3);
//        schema.addVarcharField("lxdh", 11);
//        schema.addVarcharField("dth", 12);
//        schema.addIntField("reservel");
//        schema.addIntField("reservel2");
//        schema.addIntField("reservel3");
//        schema.addVarcharField("ssdwdm",12);
//        schema.addVarcharField("ssdwmc", 5);
//        schema.addVarcharField("teamno", 8);
//        return schema;
//    }
//
//    public DataSchema initRectangle() {
//        DataSchema dataSchema = new DataSchema();
//        dataSchema.addVarcharField("type", 8);
//        dataSchema.addVarcharField("leftTop",15);
//        dataSchema.addVarcharField("rightBottom", 15);
//        dataSchema.addVarcharField("geoStr", 30);
//        dataSchema.addVarcharField("longitude", 10);
//        dataSchema.addVarcharField("latitude", 10);
//        dataSchema.addVarcharField("radius", 10);
//        return dataSchema;
//    }
//}
