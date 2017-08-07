package ui;

import indexingTopology.api.client.SystemStateQueryClient;
import indexingTopology.common.SystemState;

import javax.servlet.annotation.WebServlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.*;
import java.util.Random;
import net.sf.json.JSONObject;
import net.sf.json.JSONArray;
/**
 * Created by billlin on 2017/7/27.
 */

@WebServlet("/clientTest")
public class clientTest  extends HttpServlet {
    /**
     * 基于TCP协议的Socket通信，实现用户登录，服务端
     */
    public Thread client;

    public static double[] tupleList;

    public clientTest() {
        // TODO Auto-generated constructor stub
    }
    public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
        doPost(request, response);
    }


    public void doPost(HttpServletRequest request,HttpServletResponse response)throws IOException,ServletException{
        response.setContentType("text/html; charset=UTF-8");

//        response.setContentType("application/json; charset=utf-8");
//        SystemStateQueryClient sys = new SystemStateQueryClient("localhost",20000);
//        sys.connect();
//        SystemState systemState = null;
////        QueryCoordinatorBolt systemConfig = new QueryCoordinatorBolt();
//        try {
//            systemState = sys.query();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.out.println("throughput: " + systemState.throughout);
//        System.out.println("lastThroughput[1] = "+systemState.lastThroughput[1]);
//        double[] throughputList = systemState.lastThroughput;
//        sys.close();
        System.out.println("tests");
        double[] throughputList = new double[6];
        for (int i = 0; i < 6; i++) {
            throughputList[i] = new Random().nextDouble() * 1000;
        }

        //将每个vehicle对象拼接为json格式的对象,用于命令下发
        SystemState systemState = new SystemState();
        systemState.throughout = 100;
        systemState.lastThroughput = new double[]{1,2,3,4,5,6};
        jsonTest jsontest = new jsonTest();
        jsontest.name = "Rolf";
        jsontest.throughout = 15;
        JSONObject json = JSONObject.fromObject(jsontest); //v即对象
        System.out.println("json: "+json);
        System.out.println("systemState: "+jsontest.throughout);
//        jsonArray.add(json);
        //System.out.println(jsonArray.toString());


         //这样这个json对象就传到你发送请求的那个jsp上面。
        //而$.post(url,datas.function(data){})这里的data就是后台返回的值，也就是这里的json
        //只需要 var json = data;//就获取到了。
        //再取得原来Student的属性 var name = json.name(student 的name 属性);

        String sendCommandList = json.toString();
        request.getSession().setAttribute("tupleList", throughputList);
        System.out.println(sendCommandList);
//        request.getSession().setAttribute("systemState", systemState);
//        System.out.println(systemState.hashMap.get("dataChunkDir"));
//        System.out.println("tuple v/s: "+tupleList[0]);
        request.getSession().setMaxInactiveInterval(6);
        response.sendRedirect("gentelella-master/production/index.jsp");
        }
}

