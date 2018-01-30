package indexingTopology.util.Json;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by billlin on 2017/12/24
 */
public class JsonTest {
    public String CheckJingyiJson(int caseNum){
        String Msg = null;
        Date dateOld = new Date(System.currentTimeMillis()); // 根据long类型的毫秒数生命一个date类型的时间
        String sDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateOld); // 把date类型的时间转换为string
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null; // 把String类型转换为Date类型
        try {
            date = formatter.parse(sDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        switch (caseNum){
            case 1 : { // without necessary attribute devbtype
                Msg = "{\"devbtype\":" + 11 + ",\"devstype\":\"123\",\"devid\":\"75736331\",\"city\":\"4406\",\"longitude\":"+ 113.123123 + ",\"latitude\":" + 23.874917 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 2 : { // without locationtime
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":\"123\",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 3 : { // devbtype int to String
                Msg = "{\"devbtype\":\"11\",\"devstype\":\"123\",\"devid\":\"75736331\",\"city\":\"4406\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 4 : { // longitude double to String
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":\"123\",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":\"80.888888888888888888888888888888888\",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 5 : { // devstype String to int
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":" + 123 + ",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 6 : { // devstype String to double
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":" + 123.333333333 + ",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 7 : { // only one attribute
                Msg = "{\"devbtype\":" + 10 + "}";
                break;
            }case 8 : { // too much attribute
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":\"123\",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 9 : { //error important attribute devbtype
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":\"123\",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }case 10 : { //null attribute
                Msg = "{}";
                break;
            }case 11 : { //error json schema
                Msg = "{dasdqwd}";
                break;
            }case 12 : { //only Chinese characters
                Msg = "{\"devbtype\":\"【】%测试*\"}";
                break;
            }case 13 : { //Chinese characters
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":\"123\",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 80.8888888888 + ",\"latitude\":" + 80.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\"" + currentTime + "\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"【】%测试*\",\"teamno\":\"44010001\"}";
                break;
            }case 14 : { // less attribute
                Msg = "{\"devbtype\":" + 10 + ",\"devstype\":\"123\"}";
                break;
            }case 15 : { //error unimportant attribute
                Msg = "{\"devbtype\":" + 10 + ",\"devstyaasdpe\":\"123\",\"devid\":\"0x0101\",\"city\":\"4401\",\"longitude\":"+ 110.8888888888 + ",\"latitude\":" + 20.8888888888 + ",\"altitude\":2000.0," +
                        "\"speed\":50.0,\"direction\":40.0,\"locationtime\":\""+ currentTime +"\",\"workstate\":1,\"clzl\":\"巡逻车\",\"hphm\":\"粤A39824\",\"jzlx\":1,\"jybh\":\"100011\"," +
                        "\"jymc\":\"陈国基\",\"lxdh\":\"13576123212\",\"dth\":\"SG0000000352\",\"reserve1\":null,\"reserve2\":\"\",\"reserve3\":\"\",\"ssdwdm\":\"440100000000\"," +
                        "\"ssdwmc\":\"广州市\",\"teamno\":\"44010001\"}";
                break;
            }
            default:break;
        }
        return Msg;
    }
}
