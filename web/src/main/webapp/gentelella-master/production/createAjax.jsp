<%--
  Created by IntelliJ IDEA.
  User: billlin
  Date: 2017/8/8
  Time: 上午10:29
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>

    <title>My JSP 'createAjax.jsp' starting page</title>
    <%--<script type="text/javascript" src="js/createAjax.js"></script>--%>
</head>

<body>

<input type="button" value="获取当前时间">
<hr>
<span></span>

<script type="text/javascript">
    //1. 创建ajax对象
    var ajax;// createAjax();
    if (window.XMLHttpRequest)
    {
        //  IE7+, Firefox, Chrome, Opera, Safari 浏览器执行代码
        ajax=new XMLHttpRequest();
    }
    else
    {
        // IE6, IE5 浏览器执行代码
        ajax=new ActiveXObject("Microsoft.XMLHTTP");
    }
    //测试
    //alert(ajax!=null?"创建ajax成功！":"创建ajax失败！！");
    //2. 获取定位按钮
    var inputElement = document.getElementsByTagName("input")[0].onclick = function() {
        //3. 准备发送请求
        /*
         method表示发送请求的方式，例如GET或POST
         url表示发送请求的目标地址
         可选的boolean值
         >>true：表示该请求是异步的，这是默认值，web2.0
         >>false：表示该请求是同步的，web1.0
         */
        var method = "GET";
        alert("<%=path%>");
        var url = "<%=path%>/clientTest";
        ajax.open(method, url, true);
        //4. 真正发送异步请求
        /*
         content表示发送请求的内容，如果无内容的话，使用null表示
         如果有内容，写成key=value形成，例如：username=jack&password=123
         */
        var content = null;
        ajax.send(content);
        //5. ajax对象监听服务器的响应
        ajax.onreadystatechange = function() {
            //如果ajax对象，已经完全接收到了响应，
            if (ajax.readyState == 4) {
                //如果响应正确
                if (ajax.status == 200) {
                    var nowStr = ajax.responseText;
                    //将获取到的时间放在span标签内
                    //定位span标签
                    var spanElement = document.getElementsByTagName("span")[0];
                    //将nowStr放当span标签内
                    spanElement.innerHTML = nowStr;
                }

            }

        };

    };
</script>

</body>
</html>
