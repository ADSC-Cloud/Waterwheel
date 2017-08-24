package ui;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;



import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

import javax.ejb.Local;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
/**
 * Created by billlin on 2017/8/7.
 */
@WebServlet("/jsonTest")
public class jsonTest extends HttpServlet implements Serializable {

    public double[] throughout;
    //姓名
    private String name;
    //年龄
    private String age;
    //住址
    private String address;
    public double[] getThroughout() {
        return throughout;
    }

    public HashMap<String,String> hashMap;
    public void setHashMap(String k,String v){
        if(hashMap == null){
            hashMap = new HashMap<>();
        }
        this.hashMap.put(k,v);
    }
    public HashMap<String,String> getHashMap(){
        return hashMap;
    }
    public void setThroughout(double[] throughout) {
        if(throughout == null){
            throughout = new double[6];
        }
        this.throughout = throughout;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getAge() {
        return age;
    }
    public void setAge(String age) {
        this.age = age;
    }
    public String getAddress() {
        return address;
    }
    public void setAddress(String address) {
        this.address = address;
    }
    @Override
    public String toString() {
        return "Student [name=" + name + ", age=" + age + ", address="
                + address + ",throughput= "+throughout+ ",hashMap= "+hashMap+"]";
    }
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        //获取当前时间
        Date date = new Date();
        //创建时间格式
        DateFormat format = DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.DEFAULT, Locale.CHINA);
        String nowStr = format.format(date);

        ////设置输出内容类型和编码方式
        response.setContentType("text/html; charset=utf-8");
        //获取输出流对象
        PrintWriter pw = response.getWriter();
        //通过流对象，将信息输出到AJAX对象
        pw.write(nowStr);

    }
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    public static void main(String[] args) {
//        jsonTest stu=new jsonTest();
//        double[] a = new double[]{1,2,3,4,5,6};
//        stu.setThroughout(a);
//        String aa = "dirrrrrrrrrrr";
//        stu.setHashMap("11",aa);
//        stu.setHashMap("22","bb");
//        stu.name = "j";
////        stu.setName("JSON");
//        stu.setAge("23");
//        stu.setAddress("北京市西城区");
////1、使用JSONObject
//        JSONObject json = JSONObject.fromObject(stu);
////2、使用JSONArray
//        JSONArray array=JSONArray.fromObject(stu);
//        String strJson=json.toString();
//        String strArray=array.toString();
//        System.out.println("strJson:"+strJson);
//        System.out.println("strArray:"+strArray);
//        int num = 0;
//        int test = 1;
//        int[] arr = new int[6];
//        while(test <= 10) {
//            if (num >= 6) {
//                for(int j=0;j<5;j++){
//                    arr[j] = arr[j+1];
//                }
//                num++;
//                arr[5] = num;
//            } else {
//                arr[num++] = num;
//            }
//            for (int j = 0;j<6;j++){
//                System.out.print(arr[j]+" ");
//            }
//            System.out.println("\n");
//            test++;
//        }
        int x,y,z;
        x = y = z = 0;
        int a = x&y&z;
        System.out.println(a);
    }

}
