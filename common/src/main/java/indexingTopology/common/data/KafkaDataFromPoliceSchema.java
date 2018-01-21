package indexingTopology.common.data;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by billlin on 2017/12/29
 */
public class KafkaDataFromPoliceSchema implements Serializable {
    private int devbtype;
    private String devstype;
    private String devid;
    private String city;
    private double longitude;
    private double latitude;
    private double altitude;
    private double speed;
    private double direction;
    private String locationtime;
    private int workstate ;
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


    public boolean checkDataIntegrity(JSONObject jsonObject){
        if(jsonObject.get("locationtime") == null){
            System.out.println("Record error : consumer record timestamp is null!The record is :" + jsonObject);
            return false;
        }
        String dateValue = (String) jsonObject.get("locationtime");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = null;
        try {
            date = dateFormat.parse(dateValue);
        } catch (ParseException e) {
            System.out.println("Record error : consumer record timestamp format exception!The record is :" + jsonObject);
            return false;
        }
        jsonObject.remove("locationtime");
        jsonObject.put("locationtime", date.getTime());
        if(jsonObject.get("longitude") == null || jsonObject.get("latitude") == null){
            System.out.println("Record error : consumer record longitude or latitude is null!The record is :" + jsonObject);
            return false;
        }
        try{
            if(jsonObject.get("devbtype") != null){
                this.devbtype = jsonObject.getInteger("devbtype");
            }
            if(jsonObject.get("devstype") != null){
                this.devstype = jsonObject.getString("devstype");
            }
            if(jsonObject.get("devid") != null){
                this.devid = jsonObject.getString("devid");
            }
            if(jsonObject.get("city") != null){
                this.city = jsonObject.getString("city");
            }
            if(jsonObject.get("longitude") != null){
                this.longitude = jsonObject.getDouble("longitude");
            }
            if(jsonObject.get("latitude") != null){
                this.latitude = jsonObject.getDouble("latitude");
            }
            if(jsonObject.get("altitude") != null){
                this.altitude = jsonObject.getDouble("altitude");
            }
            if(jsonObject.get("speed") != null){
                this.speed = jsonObject.getDouble("speed");
            }
            if(jsonObject.get("locationtime") != null){
                this.locationtime = jsonObject.getString("locationtime");
            }
            if(jsonObject.get("workstate") != null){
                this.workstate = jsonObject.getInteger("workstate");
            }
            if(jsonObject.get("clzl") != null){
                this.clzl = jsonObject.getString("clzl");
            }
            if(jsonObject.get("hphm") != null){
                this.hphm = jsonObject.getString("hphm");
            }
            if(jsonObject.get("jzlx") != null){
                this.jzlx = jsonObject.getInteger("jzlx");
            }
            if(jsonObject.get("jybh") != null){
                this.jybh = jsonObject.getString("jybh");
            }
            if(jsonObject.get("jymc") != null){
                this.jymc = jsonObject.getString("jymc");
            }
            if(jsonObject.get("lxdh") != null){
                this.lxdh = jsonObject.getString("lxdh");
            }
            if(jsonObject.get("ssdwdm") != null){
                this.ssdwdm = jsonObject.getString("ssdwdm");
            }
            if(jsonObject.get("ssdwmc") != null){
                this.ssdwmc = jsonObject.getString("ssdwmc");
            }
            if(jsonObject.get("teamno") != null){
                this.teamno = jsonObject.getString("teamno");
            }
            if(jsonObject.get("dth") != null){
                this.dth = jsonObject.getString("dth");
            }
            if(jsonObject.get("reserve1") != null){
                this.reserve1 = jsonObject.getString("reserve1");
            }
            if(jsonObject.get("reserve2") != null){
                this.reserve2 = jsonObject.getString("reserve2");
            }
            if(jsonObject.get("reserve3") != null){
                this.reserve3 = jsonObject.getString("reserve3");
            }
        }catch(NullPointerException e){
            e.printStackTrace();
        }catch(NumberFormatException e){
            System.out.println("Record error : consumer record value format exception!The record is :" + jsonObject);
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        KafkaDataFromPoliceSchema kafkaDataSchema = new KafkaDataFromPoliceSchema();
        String Msg = "{\"devbtype\":" + 1 + ",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 1.11111 +",\"latitude\":" + 2 + ",\"altitude\":2000.0," +
                "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\"2017-11-12 10:10:10\",\"workstate\":11,\"jzlx\":11,\"jybh\":\"100011\",\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\",,\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
        try {
            JSONObject jsonObject = JSONObject.parseObject(Msg);
            kafkaDataSchema.checkDataIntegrity(jsonObject);
        }catch (Exception e){
            e.printStackTrace();
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String str = null;
        try {
            Date date = dateFormat.parse(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
