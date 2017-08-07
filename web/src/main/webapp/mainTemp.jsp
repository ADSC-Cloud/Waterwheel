<%@ page import="javax.xml.crypto.Data" %>
<%@ page import="model.DataBean" %><%--
  Created by IntelliJ IDEA.
  User: Hzl
  Date: 2017/8/4
  Time: 14:08
  To change this template use File | Settings | File Templates.
--%>
<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<html lang="zh">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Search</title>
    <link href="${pageContext.request.contextPath}/style/diary.css" rel="stylesheet">
    <link href="${pageContext.request.contextPath}/bootstrap/css/bootstrap.css" rel="stylesheet">
    <link href="${pageContext.request.contextPath}/bootstrap/css/bootstrap-responsive.css" rel="stylesheet">
    <script src="${pageContext.request.contextPath}/bootstrap/js/bootstrap.js"></script>
    <script src="${pageContext.request.contextPath}/bootstrap/js/jQuery.js"></script>
    <style type="text/css">
        body {
            padding-top: 60px;
            padding-bottom: 40px;
        }
    </style>
    <script type="text/javascript">
        function saveData() {
            //alert("hehe")
            <%
                if(request.getSession().getAttribute("dataBean") != null){
                System.out.println("save");
                request.getSession().setAttribute("oldDataBean",request.getSession().getAttribute("dataBean"));
                }
            %>

        }
    </script>
</head>
<body onload="saveData()">
<div class="navbar navbar-inverse navbar-fixed-top">
    <div class="navbar-inner">
        <div class="container">
            <form name="myForm" class="navbar-form pull-right" method="post" action="main?all=true">
                <input class="span2" id="s_title" name="s_title"  type="text" style="margin-top:5px;height:30px;" placeholder="Search">
                <button type="submit" class="btn" onkeydown="if(event.keyCode==13) myForm.submit()"><i class="icon-search"></i>&nbsp;Search</button>
            </form>
        </div>
    </div>
</div>
<div class="container">
    <%--<nav aria-label="Page navigation">
        <ul class="pagination">--%>
            <div class="row-fluid">
                <div class="span9">
                    <jsp:include page="${mainPage }"></jsp:include>
                </div>
            </div>
</div>
</body>
</html>
