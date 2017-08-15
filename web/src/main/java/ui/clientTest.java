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
import java.util.*;

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
//    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
//        // TODO Auto-generated method stub
//        doPost(request, response);
//    }

    public void doPost(HttpServletRequest request,HttpServletResponse response)throws IOException,ServletException{
        response.setContentType("text/html; charset=UTF-8");

////        response.setContentType("application/json; charset=utf-8");
        SystemStateQueryClient sys = new SystemStateQueryClient("localhost",20000);
        sys.connect();
        SystemState systemState = null;
//        QueryCoordinatorBolt systemConfig = new QueryCoordinatorBolt();
        try {
            systemState = sys.query();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(systemState.lastThroughput[0]+" "+systemState.lastThroughput[1]+" "+systemState.lastThroughput[2]+" "+systemState.lastThroughput[3]+" "+systemState.lastThroughput[4]+" "+systemState.lastThroughput[5]);
        double[] throughputList = systemState.lastThroughput;
        sys.close();
        System.out.println("tests");
//        double[] throughputList = new double[6];
//        for (int i = 0; i < 6; i++) {
//            throughputList[i] = new Random().nextDouble() * 1000;
//        }
////        //获取输出流对象
////        PrintWriter pw = response.getWriter();
////        //通过流对象，将信息输出到AJAX对象
////        pw.write(String.valueOf(throughputList));
//////        将每个vehicle对象拼接为json格式的对象,用于命令下发
//        SystemState systemState = new SystemState();
//        systemState.setThroughout(20.0);
//        systemState.setLastThroughput(throughputList);
        systemState.setTreeMap("1","/Users/billlin/机器学习asd/分布式/tmp/append-only-store/dataDir");

        systemState.setTreeMap("10","10");
        systemState.setTreeMap("11","11");
        systemState.setTreeMap("12","12");
        systemState.setTreeMap("14","14");
        systemState.setTreeMap("17","17");
        systemState.setTreeMap("23","23");
        systemState.setTreeMap("b12","23");
        systemState.setTreeMap("b21","23");
        systemState.setTreeMap("a12","23");
        systemState.setTreeMap("a21","23");
        systemState.setTreeMap("18","18");
        systemState.setTreeMap("13","13");
        systemState.setTreeMap("21","21");
//        List<Map.Entry<String,String>> list222 = new ArrayList<Map.Entry<String,String>>(systemState.getHashMap().entrySet());
//        //然后通过比较器来实现排序
//        Collections.sort(list222,new Comparator<Map.Entry<String,String>>() {
//            //升序排序
//            public int compare(Map.Entry<String, String> o1,
//                               Map.Entry<String, String> o2) {
//
//                return o1.getKey().toLowerCase().compareTo(o2.getKey().toLowerCase());
//            }
//
//        });
//        HashMap<String,String> sortHashMap = new HashMap<String,String>();
//        for(Map.Entry<String,String> mapping:list222){
//            System.out.println("key: "+mapping.getKey()+",value: "+mapping.getValue());
//            sortHashMap.put(mapping.getKey(),mapping.getValue());
//        }
        Iterator it123 = systemState.getTreeMap().entrySet().iterator();
        while (it123.hasNext()) {
            Map.Entry entry = (Map.Entry) it123.next();
            String key = (String) entry.getKey();
            String val = (String)entry.getValue();
            System.out.println(key+" : "+val);
        }
//        systemState.changeHashMap(sortHashMap);
        //turn to map test
//        Map<String,String> map = systemState.getHashMap();
//        Iterator iter = map.entrySet().iterator();
//        List<String> list = new ArrayList<String>();
//        while (iter.hasNext()) {
//            Map.Entry entry = (Map.Entry) iter.next();
//            String key = (String) entry.getKey();
//            String val = (String)entry.getValue();
//            list.add(key);
//        }
//        String source[] = { "dad", "bood", "bada", "Admin", "Aa ", "A ", "Good", "aete", "cc", "Ko", "Beta", "Could" };
//        List<String> list = Arrays.asList(source);

//<<<<<<< HEAD


//        System.out.println(list);
//        systemState.setHashMap("3","/Users/billlin/机器学习/分布sa式/tmp/append-only-store/dataDir");
//        systemState.setHashMap("4","/Users/billlin/机器学习/分布式qwd/tmp/append-only-store/metadataDir");
//        systemState.setHashMap("5","/Users/billlin/机器学习/分布d式/tmp/append-only-store/dataDir");
//        systemState.setHashMap("6","/Users/billlin/机器学习/分布式/qwdtmp/append-only-store/metadataDir");
//        systemState.setHashMap("7","/Users/billlin/机器学习/分布式/wtmp/append-only-store/dataDir");
//        systemState.setHashMap("8","/Users/billlin/机器学习/分布式/tmpqrweew/append-only-store/metadataDir");
//        systemState.setHashMap("9","/Users/billlin/机器学习/分布式/tmp/appeqeqwwnd-only-store/dataDir");
//        systemState.setHashMap("10","/Users/billlin/机器学习/分布式/tmp/appendd-oadswnly-store/metadataDir");
//        systemState.setHashMap("11","/Users/billlin/机器学习/分布式/tmp/append-edaonlyd-store/dataDir");
//        systemState.setHashMap("12","/Users/billlin/机器学习/分布式/tmp/append-only-sqwetore/metadataDir");
//        systemState.setHashMap("13","/Users/billlin/机器学习/分布式/tmp/append-only-stordeade/dataDir");
//        systemState.setHashMap("14","/Users/billlin/机器学习/分布式/tmp/append-only-store/meeqdtadataDir");
//        systemState.setHashMap("15","/Users/billlin/机器学习/分布式/tmp/append-only-storede/asdqdataDir");
//        systemState.setHashMap("16","/Users/billlin/机器学习/分布式/tmp/append-only-store/metadadssdfataDir");
//        systemState.setHashMap("17","/Users/billlin/机器学习/分布式/tmp/append-only-store/datsdccaDir");
//        systemState.setHashMap("18","/Users/billlin/机器学习/分布式/tmp/append-only-store/dadctaDir");
//        systemState.setHashMap("19","/Users/billlin/机器学习/分布式/tmp/append-only-store/metadasddsctaDir");
//        systemState.setHashMap("20","/Users/billlin/机器学习/分布式/tmp/append-only-store/dataDdqwir");
//        systemState.setHashMap("21","/Users/billlin/机器学习/分布式/tmp/append-only-stcsdffore/metadataDir");
//        systemState.setCpuRatio(20);
        JSONObject json = JSONObject.fromObject(systemState); //v即对象
        String jsonStr = json.toString();
        System.out.println(json.toString());
        response.getWriter().write(jsonStr);
//        Log4jInit.ysulogger.debug(json.toString());
//        System.out.println("json: "+json);
//        System.out.println("systemState: "+jsontest.throughout);
//=======
//        //将每个vehicle对象拼接为json格式的对象,用于命令下发
//        SystemState systemState = new SystemState();
//        systemState.throughout = 100;
//        systemState.lastThroughput = new double[]{1,2,3,4,5,6};
///*        jsonTest jsontest = new jsonTest();
//        jsontest.name = "Rolf";
//        jsontest.throughout = 15;*/
//        JSONObject json = JSONObject.fromObject(systemState); //v即对象
///*        System.out.println("json: "+json);
//        System.out.println("systemState: "+jsontest.throughout);*/
//>>>>>>> zlin/master
////        jsonArray.add(json);
////        System.out.println(jsonArray.toString());
//
//
//         //这样这个json对象就传到你发送请求的那个jsp上面。
//        //而$.post(url,datas.function(data){})这里的data就是后台返回的值，也就是这里的json
//        //只需要 var json = data;//就获取到了。
//        //再取得原来Student的属性 var name = json.name(student 的name 属性);
//
////        request.getSession().setAttribute("tupleList", json);
////        request.getSession().setAttribute("systemState", systemState);
////        request.getSession().setAttribute("systemState", systemState);
////        System.out.println(systemState.hashMap.get("dataChunkDir"));
////        System.out.println("tuple v/s: "+tupleList[0]);
////        request.getSession().setMaxInactiveInterval(6);
////        response.sendRedirect("gentelella-master/production/index.jsp");
        }
}
