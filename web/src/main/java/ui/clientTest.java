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
        SystemStateQueryClient sys = new SystemStateQueryClient("localhost",20000);
        sys.connect();
        SystemState systemState = null;
//        QueryCoordinatorBolt systemConfig = new QueryCoordinatorBolt();
        try {
            systemState = sys.query();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("throughput: " + systemState.throughout);
        System.out.println("lastThroughput[1] = "+systemState.lastThroughput[1]);
        double[] throughputList = systemState.lastThroughput;
        sys.close();
//        double[] throughputList = new double[6];
//        for (int i = 0; i < 6; i++) {
//            throughputList[i] = new Random().nextDouble() * 1000;
//        }
        request.getSession().setAttribute("tupleList", throughputList);
//        System.out.println("tuple v/s: "+tupleList[0]);
        request.getSession().setMaxInactiveInterval(6);
        response.sendRedirect("index.jsp");
    }
}

