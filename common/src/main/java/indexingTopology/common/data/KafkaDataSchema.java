package indexingTopology.common.data;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by billlin on 2017/12/29
 */
public class KafkaDataSchema implements Serializable {
    private int devbtype;
    private String devid;
    private String city;
    private double longitude;
    private double latitude;
    private double altitude;
    private double speed;
    private double direction;
    private String locationtime;
    private int workstate ;
    private int jzlx;
    private String jybh;
    private String jymc;
    private String ssdwdm;
    private String ssdwmc;
    private String teamno;
    private String reserve1;

    public void checkDataIntegrity(JSONObject jsonObject) throws Exception{
            this.devbtype = jsonObject.getInteger("devbtype");
            this.devid = jsonObject.getString("devid");
            this.city =  jsonObject.getString("city");
            this.longitude = jsonObject.getDouble("longitude");
            this.latitude = jsonObject.getDouble("latitude");
            this.altitude = jsonObject.getDouble("altitude");
            this.speed = jsonObject.getDouble("speed");
            this.direction = jsonObject.getDouble("direction");
            this.locationtime = jsonObject.getString("locationtime");
            this.workstate = jsonObject.getInteger("workstate");
            this.jzlx = jsonObject.getInteger("jzlx");
            this.jybh = jsonObject.getString("jybh");
            this.jymc = jsonObject.getString("jymc");
            this.ssdwdm = jsonObject.getString("ssdwdm");
            this.ssdwmc = jsonObject.getString("ssdwmc");
            this.teamno = jsonObject.getString("teamno");
            this.reserve1 = jsonObject.getString("reserve1");
    }

    public static void main(String[] args) {
        KafkaDataSchema kafkaDataSchema = new KafkaDataSchema();
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
        System.out.println("as");
    }
}
