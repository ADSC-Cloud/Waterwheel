package ui;


import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import indexingTopology.common.aggregator.*;
import indexingTopology.common.data.DataTuple;
import model.DataBean;
import model.PostPredicator;

@WebServlet("/main")
public class MainServlet extends HttpServlet{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private DataBean dataBean;

    private int xLow = 0;

    private int xHigh = 0;

    private int yLow = 0;

    private int yHigh = 0;

    private int time = 0;

    SearchTest searchTest;

    int sum_len = 0, max_len = 0, min_len = 0;
    int len = 0;
    String groupby = null;
    String count = null;
    String[] sum = null;
    String[] max = null;
    String[] min = null;
    String postPredicateName = null;
    String postPredicateWay = null;
    String postPredicateDigital = null;
    PostPredicator postPredicator = null;

    int rows;
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        this.doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        request.setCharacterEncoding("utf-8");
        /**
         * test hzl
         */


//        String page=request.getParameter("page");
        DataBean oldDataBean = (DataBean) request.getSession().getAttribute("oldDataBean");
        xLow = Integer.parseInt(request.getParameter("xLow"));
        xHigh = Integer.parseInt(request.getParameter("xHigh"));
        yLow = Integer.parseInt(request.getParameter("yLow"));
        yHigh = Integer.parseInt(request.getParameter("yHigh"));
        time = Integer.parseInt(request.getParameter("time"));
        groupby = request.getParameter("groupby");
        count = request.getParameter("count");
      /*  postPredicateName = request.getParameter("fieldNamesPostSelect");
        postPredicateWay = request.getParameter("equal");
        postPredicateDigital = request.getParameter("digital");
        Pattern pattern = Pattern.compile("^[0-9]+(.[0-9]+)?$");
        double predicateNum = 0;
        if(!postPredicateDigital.equals("null")){
            if(pattern.matcher(postPredicateDigital).matches()) {
                predicateNum = Double.parseDouble(postPredicateDigital);
                postPredicator = new PostPredicator(postPredicateName, postPredicateWay, predicateNum);
            }
            else
                System.out.println("Not Number");
        }
        else {
            System.out.println("null");
        }
        postPredicator = new PostPredicator("zcode", ">", 200);*/

//        if(request.getParameterValues("min") != null)
        min = request.getParameterValues("min");
//        if(request.getParameterValues("max") != null)
        max = request.getParameterValues("max");
//        if(request.getParameterValues("sum") != null)
        sum = request.getParameterValues("sum");
        //System.out.println("min is : "+ request.getParameterValues("min")[0] + request.getParameterValues("min")[1]);
        System.out.println(xLow+ " " + yLow + " " + xHigh + " " + yHigh + " " + time);
        if(!count.equals("null")){
            len = 1;
        }else len = 0;
        if(sum != null) {
            sum_len = sum.length;
        }else sum_len = 0;
        if(max != null) {
            max_len = max.length;
        }else max_len = 0;
        if(min != null){
            min_len = min.length;
        }else min_len = 0;

        len = len + sum_len + max_len + min_len;
        System.out.println("shuahdushdushudhsud : " + len);
        AggregateField[] fields = new AggregateField[len];
        System.out.println(len);
        int i = 0;
        if(!count.equals("null"))
            fields[i++] = new AggregateField(new Count(), request.getParameter("count"));
        if(sum != null)
            for (int i2 = 0; i2 < sum_len; i2++)
                fields[i++] = new AggregateField(new Sum(), request.getParameterValues("sum")[i2]);
        if(max != null)
            for (int i3 = 0; i3 < max_len; i3++)
                fields[i++] = new AggregateField(new Max(), request.getParameterValues("max")[i3]);
        if(min != null)
            for (int i4 = 0; i4 < min_len; i4++)
                fields[i++] = new AggregateField(new Min(), request.getParameterValues("min")[i4]);
        searchTest = new SearchTest(xLow, xHigh, yLow, yHigh, time, groupby, fields);
        dataBean = searchTest.executeQuery();

        List<DataTuple> diaryList = dataBean.getTuples();
        List<String> fieldNames = dataBean.getFieldNames();
        long queryTime = dataBean.getTime();
        String time = String.valueOf(queryTime);
        String size = String.valueOf(fieldNames.size());
/*            for(int i=0;i<rows;i++){
                diaryList.add(""+i);
            }*/
//            int total=rows;
//            String pageCode=this.genPagation(total, Integer.parseInt(page), 4);
        request.getSession().setAttribute("dataBean", dataBean);

//            request.setAttribute("pageCode", pageCode);
        request.setAttribute("listSize",size);
        request.setAttribute("diaryList", diaryList);
        request.setAttribute("fieldNames", fieldNames);
        request.setAttribute("queryTime",time);
        request.setAttribute("numberOfTuples", diaryList.size());
        request.getRequestDispatcher("tables_dynamic.jsp").forward(request,response);

    }




}
