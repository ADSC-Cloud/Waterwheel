<%--
  Created by IntelliJ IDEA.
  User: billlin
  Date: 2017/8/6
  Time: 下午2:06
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="indexingTopology.*" %>
<%@ page import="javax.servlet.ServletRequest" %>
<%@ page import="indexingTopology.common.SystemState" %>
<%@ page import="net.sf.json.JSONObject" %>
<%@ page import="net.sf.json.JSONArray" %>
<%@ page import="ui.clientTest" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="java.util.Map" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <!-- Meta, title, CSS, favicons, etc. -->
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>DITIR Web UI</title>

    <!-- Bootstrap -->
    <link href="gentelella-master/vendors/bootstrap/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Font Awesome -->
    <link href="gentelella-master/vendors/font-awesome/css/font-awesome.min.css" rel="stylesheet">
    <!-- NProgress -->
    <link href="gentelella-master/vendors/nprogress/nprogress.css" rel="stylesheet">
    <!-- iCheck -->
    <link href="gentelella-master/vendors/iCheck/skins/flat/green.css" rel="stylesheet">



    <link href="gentelella-master/vendors/datatables.net-bs/css/dataTables.bootstrap.min.css" rel="stylesheet">
    <link href="gentelella-master/vendors/datatables.net-buttons-bs/css/buttons.bootstrap.min.css" rel="stylesheet">
    <link href="gentelella-master/vendors/datatables.net-fixedheader-bs/css/fixedHeader.bootstrap.min.css" rel="stylesheet">
    <link href="gentelella-master/vendors/datatables.net-responsive-bs/css/responsive.bootstrap.min.css" rel="stylesheet">
    <link href="gentelella-master/vendors/datatables.net-scroller-bs/css/scroller.bootstrap.min.css" rel="stylesheet">

    <!-- bootstrap-progressbar -->
    <link href="gentelella-master/vendors/bootstrap-progressbar/css/bootstrap-progressbar-3.3.4.min.css" rel="stylesheet">
    <!-- JQVMap -->
    <link href="gentelella-master/vendors/jqvmap/dist/jqvmap.min.css" rel="stylesheet"/>
    <!-- bootstrap-daterangepicker -->
    <link href="gentelella-master/vendors/bootstrap-daterangepicker/daterangepicker.css" rel="stylesheet">

    <!-- Custom Theme Style -->
    <link href="gentelella-master/build/css/custom.min.css" rel="stylesheet">

    <%--<link rel="stylesheet" href="../../js/jquery-1.10.2.min.js">--%>

    <!-- jQuery -->
    <script src="gentelella-master/vendors/jquery/dist/jquery.min.js"></script>

    <!-- Bootstrap -->
    <script src="gentelella-master/vendors/bootstrap/dist/js/bootstrap.min.js"></script>
    <!-- FastClick -->
    <script src="gentelella-master/vendors/fastclick/lib/fastclick.js"></script>
    <!-- NProgress -->
    <script src="gentelella-master/vendors/nprogress/nprogress.js"></script>
    <!-- iCheck -->
    <script src="gentelella-master/vendors/iCheck/icheck.min.js"></script>

    <!-- ECharts -->
    <script src="gentelella-master/vendors/echarts/dist/echarts.min.js"></script>
    <script src="gentelella-master/vendors/echarts/map/js/world.js"></script>
    <!-- Chart.js -->
    <script src="gentelella-master/vendors/Chart.js/dist/Chart.min.js"></script>
    <!-- Datatables -->
    <%--<script src="../vendors/datatables.net/js/jquery.dataTables.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-bs/js/dataTables.bootstrap.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-buttons/js/dataTables.buttons.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-buttons-bs/js/buttons.bootstrap.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-buttons/js/buttons.flash.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-buttons/js/buttons.html5.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-buttons/js/buttons.print.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-fixedheader/js/dataTables.fixedHeader.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-keytable/js/dataTables.keyTable.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-responsive/js/dataTables.responsive.min.js"></script>--%>
    <%--<script src="../vendors/datatables.net-responsive-bs/js/responsive.bootstrap.js"></script>--%>
    <%--<script src="../vendors/datatables.net-scroller/js/dataTables.scroller.min.js"></script>--%>
    <%--<script src="../vendors/jszip/dist/jszip.min.js"></script>--%>
    <%--<script src="../vendors/pdfmake/build/pdfmake.min.js"></script>--%>
    <%--<script src="../vendors/pdfmake/build/vfs_fonts.js"></script>--%>
    <!-- Custom Theme Scripts -->
    <%--<script src="../build/js/custom.min.js"></script>--%>

    <!-- gauge.js -->
    <script src="gentelella-master/vendors/gauge.js/dist/gauge.min.js"></script>
    <!-- bootstrap-progressbar -->
    <script src="gentelella-master/vendors/bootstrap-progressbar/bootstrap-progressbar.min.js"></script>
    <!-- iCheck -->
    <script src="gentelella-master/vendors/iCheck/icheck.min.js"></script>
    <!-- Skycons -->
    <script src="gentelella-master/vendors/skycons/skycons.js"></script>
    <!-- Flot -->
    <script src="gentelella-master/vendors/Flot/jquery.flot.js"></script>
    <script src="gentelella-master/vendors/Flot/jquery.flot.pie.js"></script>
    <script src="gentelella-master/vendors/Flot/jquery.flot.time.js"></script>
    <script src="gentelella-master/vendors/Flot/jquery.flot.stack.js"></script>
    <script src="gentelella-master/vendors/Flot/jquery.flot.resize.js"></script>
    <!-- Flot plugins -->
    <script src="gentelella-master/vendors/flot.orderbars/js/jquery.flot.orderBars.js"></script>
    <script src="gentelella-master/vendors/flot-spline/js/jquery.flot.spline.min.js"></script>
    <script src="gentelella-master/vendors/flot.curvedlines/curvedLines.js"></script>
    <!-- DateJS -->
    <script src="gentelella-master/vendors/DateJS/build/date.js"></script>
    <!-- JQVMap -->
    <script src="gentelella-master/vendors/jqvmap/dist/jquery.vmap.js"></script>
    <script src="gentelella-master/vendors/jqvmap/dist/maps/jquery.vmap.world.js"></script>
    <script src="gentelella-master/vendors/jqvmap/examples/js/jquery.vmap.sampledata.js"></script>
    <!-- bootstrap-daterangepicker -->
    <script src="gentelella-master/vendors/moment/min/moment.min.js"></script>
    <script src="gentelella-master/vendors/bootstrap-daterangepicker/daterangepicker.js"></script>

    <!-- jQuery Sparklines -->
    <script src="gentelella-master/vendors/jquery-sparkline/dist/jquery.sparkline.min.js"></script>
    <!-- easy-pie-chart -->
    <script src="gentelella-master/vendors/jquery.easy-pie-chart/dist/jquery.easypiechart.js?ver=1"></script>
    <!-- bootstrap-progressbar -->
    <script src="gentelella-master/vendors/bootstrap-progressbar/bootstrap-progressbar.min.js"></script>
</head>

<body class="nav-md">

<div class="container body">
    <div class="main_container">
        <div class="col-md-3 left_col">
            <div class="left_col scroll-view">
                <div class="navbar nav_title" style="border: 0;">
                    <a href="index.jsp" class="site_title" style="padding-left: 50px;"><span>DITIR</span></a>
                </div>

                <div class="clearfix"></div>

                <!-- menu profile quick info -->
                <%--<div class="profile clearfix">--%>
                    <%--<div class="profile_pic">--%>
                        <%--<img src="images/img.jpg" alt="..." class="img-circle profile_img">--%>
                    <%--</div>--%>
                    <%--<div class="profile_info">--%>
                        <%--<span>Welcome,</span>--%>

                        <%--<h2>John Doe</h2>--%>
                    <%--</div>--%>
                <%--</div>--%>
                <!-- /menu profile quick info -->
                <script type="text/javascript">
                    //    var xmlhttp;
                    //    if (window.XMLHttpRequest)
                    //    {
                    //        //  IE7+, Firefox, Chrome, Opera, Safari 浏览器执行代码
                    //        xmlhttp=new XMLHttpRequest();
                    //    }

//                    //1. 创建ajax对象
//                    var ajax;// createAjax();
//                    if (window.XMLHttpRequest)
//                    {
//                        //  IE7+, Firefox, Chrome, Opera, Safari 浏览器执行代码
//                        ajax=new XMLHttpRequest();
//                    }
//                    else
//                    {
//                        // IE6, IE5 浏览器执行代码
//                        ajax=new ActiveXObject("Microsoft.XMLHTTP");
//                    }
//                    alert(ajax!=null?"创建ajax成功！":"创建ajax失败！！");
                    //2. 获取定位按钮
//                    var inputElement = document.getElementById("ajax").onclick = function() {
                     function chart() {


                        //3. 准备发送请求
                        /*
                         method表示发送请求的方式，例如GET或POST
                         url表示发送请求的目标地址
                         可选的boolean值
                         >>true：表示该请求是异步的，这是默认值，web2.0
                         >>false：表示该请求是同步的，web1.0
                         */
                        <%--var method = "GET";--%>
                        <%--var url = "<%=path%>/clientTest";--%>
                        <%--ajax.open(method, url, true);--%>
<%--//                        ajax.dataType("json");--%>
                        <%--//4. 真正发送异步请求--%>
                        <%--/*--%>
                         <%--content表示发送请求的内容，如果无内容的话，使用null表示--%>
                         <%--如果有内容，写成key=value形成，例如：username=jack&password=123--%>
                         <%--*/--%>
                        <%--var content = "111";--%>
                        <%--ajax.send(content);--%>
                        <%--//5. ajax对象监听服务器的响应--%>
                        <%--ajax.onreadystatechange = function() {--%>
                            <%--//如果ajax对象，已经完全接收到了响应，--%>
                            <%--if (ajax.readyState == 4) {--%>
                                <%--//如果响应正确--%>
                                <%--if (ajax.status == 200) {--%>
                                    <%--var nowStr = JSON.parse(ajax.responseText);--%>
                                    <%--var table = document.getElementById("datatable");--%>
                                    <%--var t1=document.getElementById("datatable");--%>
<%--//                                    var a = nowStr.hashMap[0];--%>
<%--//                                    alert(a);--%>
<%--//                                    var rowNum=t1.rows.length;--%>
<%--//                                    if(rowNum>0) {--%>
<%--//                                        for (i = 0; i < rowNum; i++) {--%>
<%--//                                            t1.deleteRow(i);--%>
<%--//                                            rowNum = rowNum - 1;--%>
<%--//                                            i = i - 1;--%>
<%--//                                        }--%>
<%--//                                    }--%>
<%--////                                    alert(nowStr.hashMap.length);--%>
<%--//                                    for(var k in nowStr.hashMap){--%>
<%--//                                        var newRow = table.insertRow(); //创建新行--%>
<%--//                                        var newCell1 = newRow.insertCell(0); //创建新单元格--%>
<%--//                                        newCell1.innerHTML = k ; //单元格内的内容--%>
<%--//                                        newCell1.setAttribute("align","center"); //设置位置--%>
<%--//                                        var newCell1 = newRow.insertCell(1); //创建新单元格--%>
<%--//                                        newCell1.innerHTML = nowStr.hashMap[k] ; //单元格内的内容--%>
<%--//                                        newCell1.setAttribute("align","center"); //设置位置--%>
<%--//                                    }--%>
                                    <%--init_flot_chart(nowStr);--%>
                                    <%--//将获取到的时间放在span标签内--%>
                                    <%--//定位span标签--%>
<%--//                                    var spanElement = document.getElementsByTagName("span")[0];--%>
                                    <%--//将nowStr放当span标签内--%>
<%--//                                    spanElement.innerHTML = nowStr;--%>
                                <%--}--%>

                            <%--}--%>

                        <%--};--%>

                    };
//                    chart();
//                    window.setInterval(chart, 2000);
                    <%JSONObject jsonStr = (JSONObject)request.getSession().getAttribute("tupleList");%>
                    <%SystemState sys = (SystemState)request.getSession().getAttribute("systemState");%>
                </script>
                <br />

                <!-- sidebar menu -->
                <div id="sidebar-menu" class="main_menu_side hidden-print main_menu">
                    <div class="menu_section">
                        <ul class="nav side-menu">
                            <li><a href="index.jsp"><i class="fa fa-home"></i> Dashboard </a>
                                <%--<ul class="nav child_menu">--%>
                                    <%--<li><a href="/web/gentelella-master/production/index.jsp">Dashboard</a></li>--%>
                                <%--</ul>--%>
                            </li>
                            <li><a href="tables_dynamic.jsp"><i class="fa fa-table"></i> Try Query </a>
                                <%--<ul class="nav child_menu">--%>
                                    <%--<li><a href="/web/tables_dynamic.jsp">Table Dynamic</a></li>--%>
                                <%--</ul>--%>
                            </li>
                        </ul>
                    </div>

                </div>
                <!-- /sidebar menu -->
                <!-- /menu footer buttons -->
            </div>
        </div>

        <!-- top navigation -->
        <div class="top_nav">
            <div class="nav_menu">
                <nav>
                    <div class="nav toggle">
                        <a id="menu_toggle"><i class="fa fa-bars"></i></a>
                    </div>

                </nav>
            </div>
        </div>
        <!-- /top navigation -->

        <!-- page content -->
        <div class="right_col" role="main" style="min-height: 650px">
            <!-- top tiles -->
            <div class="row tile_count">
                <%--<div class="col-md-12 col-sm-12 col-xs-12 tile_stats_count">--%>
                    <%--<span class="count_top"><i class="fa fa-user"></i> Total Users</span>--%>
                    <%--<h1>DITIR  <small>A distributed system for high-rate data indexing and real-time querying</small></h1>--%>
                    <%--<span class="count_bottom"><i class="green">4% </i> From last Week</span>--%>
                <%--</div>--%>
                <!--<div class="col-md-2 col-sm-4 col-xs-6 tile_stats_count">-->
                <!--<span class="count_top"><i class="fa fa-clock-o"></i> Average Time</span>-->
                <!--<div class="count">123.50</div>-->
                <!--<span class="count_bottom"><i class="green"><i class="fa fa-sort-asc"></i>3% </i> From last Week</span>-->
                <!--</div>-->
                <!--<div class="col-md-2 col-sm-4 col-xs-6 tile_stats_count">-->
                <!--<span class="count_top"><i class="fa fa-user"></i> Total Males</span>-->
                <!--<div class="count green">2,500</div>-->
                <!--<span class="count_bottom"><i class="green"><i class="fa fa-sort-asc"></i>34% </i> From last Week</span>-->
                <!--</div>-->
                <!--<div class="col-md-2 col-sm-4 col-xs-6 tile_stats_count">-->
                <!--<span class="count_top"><i class="fa fa-user"></i> Total Females</span>-->
                <!--<div class="count">4,567</div>-->
                <!--<span class="count_bottom"><i class="red"><i class="fa fa-sort-desc"></i>12% </i> From last Week</span>-->
                <!--</div>-->
                <!--<div class="col-md-2 col-sm-4 col-xs-6 tile_stats_count">-->
                <!--<span class="count_top"><i class="fa fa-user"></i> Total Collections</span>-->
                <!--<div class="count">2,315</div>-->
                <!--<span class="count_bottom"><i class="green"><i class="fa fa-sort-asc"></i>34% </i> From last Week</span>-->
                <!--</div>-->
                <!--<div class="col-md-2 col-sm-4 col-xs-6 tile_stats_count">-->
                <!--<span class="count_top"><i class="fa fa-user"></i> Total Connections</span>-->
                <!--<div class="count">7,325</div>-->
                <!--<span class="count_bottom"><i class="green"><i class="fa fa-sort-asc"></i>34% </i> From last Week</span>-->
                <!--</div>-->
            </div>
            <!-- /top tiles -->

            <div class="row">
                <div class="col-md-6 col-sm-6 col-xs-6">
                    <div class="dashboard_graph" style="padding-right: 25px;padding-left: 25px;">

                        <div class="row x_title">
                            <div class="col-md-8">
                                <h3>Overall Insertion Throughput </h3>
                            </div>
                            <div class="col-md-8">
                                <!--<div id="reportrange" class="pull-right" style="background: #fff; cursor: pointer; padding: 5px 10px; border: 1px solid #ccc">-->
                                <!--<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>-->
                                <!--<span>December 30, 2014 - January 28, 2015</span> <b class="caret"></b>-->
                                <!--</div>-->
                            </div>
                        </div>

                        <div class="col-md-12 col-sm-12 col-xs-12" style="padding-left: 0px;padding-right: 0px;">
                            <div id="echart_line" class="demo-placeholder"></div>
                        </div>
                        <!--<div class="col-md-3 col-sm-3 col-xs-12 bg-white">-->
                        <!--<div class="x_title">-->
                        <!--<h2>Top Campaign Performance</h2>-->
                        <!--<div class="clearfix"></div>-->
                        <!--</div>-->

                        <!--<div class="col-md-12 col-sm-12 col-xs-6">-->
                        <!--<div>-->
                        <!--<p>Facebook Campaign</p>-->
                        <!--<div class="">-->
                        <!--<div class="progress progress_sm" style="width: 76%;">-->
                        <!--<div class="progress-bar bg-green" role="progressbar" data-transitiongoal="80"></div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--<div>-->
                        <!--<p>Twitter Campaign</p>-->
                        <!--<div class="">-->
                        <!--<div class="progress progress_sm" style="width: 76%;">-->
                        <!--<div class="progress-bar bg-green" role="progressbar" data-transitiongoal="60"></div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--<div class="col-md-12 col-sm-12 col-xs-6">-->
                        <!--<div>-->
                        <!--<p>Conventional Media</p>-->
                        <!--<div class="">-->
                        <!--<div class="progress progress_sm" style="width: 76%;">-->
                        <!--<div class="progress-bar bg-green" role="progressbar" data-transitiongoal="40"></div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--<div>-->
                        <!--<p>Bill boards</p>-->
                        <!--<div class="">-->
                        <!--<div class="progress progress_sm" style="width: 76%;">-->
                        <!--<div class="progress-bar bg-green" role="progressbar" data-transitiongoal="50"></div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--</div>-->
                        <!--</div>-->

                        <!--</div>-->

                        <div class="clearfix"></div>
                    </div>
                </div>

                <div class="col-md-12 col-xs-12 widget widget_tally_box" style="min-width: 50%">
                    <div class="x_panel ui-ribbon-container fixed_height_320" style="height:350px;border: 0px;">
                        <div class="ui-ribbon-wrapper">
                            <!--<div class="ui-ribbon">-->
                            <!--30% Off-->
                            <!--</div>-->
                        </div>
                        <div class="x_title">
                            <h2>Cluster Resource Utlization</h2>
                            <div class="clearfix"></div>
                        </div>
                        <div class="x_content " style="height: 300px;">
                            <div class="col-md-6 col-sm-6 col-xs-6">
                                <div style="text-align: center; margin-bottom: 17px">
                                  <span class="chart" data-percent="50" id="dataper" >
                                      <span class="percent" ></span>
                                  </span>

                                    <div class="show_per1" style="margin-top: 20px"></div>
                                </div>

                                <h3 class="name_title">CPU</h3>
                            </div>
                            <div class="col-md-6 col-sm-6 col-xs-6">
                                <div style="text-align: center; margin-bottom: 17px">
                                  <span class="chart" data-percent="50" id="dataper2" >
                                      <span class="percent" ></span>
                                  </span>
                                    <div class="show_per2" style="margin-top: 20px"></div>
                                </div>

                                <h3 class="name_title">Disk</h3>
                                <%--<p>Used ratio</p>--%>

                                <%--<div class="divider"></div>--%>
                                <%--<p>The status is refreshed after 5 seconds</p>--%>
                            </div>

                            <%--<p style="width: 100px;"></p>--%>

                            <br/>
                            <div class="divider" style="margin-top: 200px;"></div>
                            <p >The status is refreshed every 5 seconds.</p>
                        </div>
                    </div>
                </div>
                <%--<div class="col-md-1 col-xs-12 widget widget_tally_box" style="min-width: 240px">--%>
                    <%--<div class="x_panel ui-ribbon-container fixed_height_320" style="height:350px;border: 0px;">--%>
                        <%--<div class="ui-ribbon-wrapper">--%>
                            <%--<!--<div class="ui-ribbon">-->--%>
                            <%--<!--30% Off-->--%>
                            <%--<!--</div>-->--%>
                        <%--</div>--%>
                        <%--<div class="x_title">--%>
                            <%--<h2>The system state</h2>--%>
                            <%--<div class="clearfix"></div>--%>
                        <%--</div>--%>
                        <%--<div class="x_content">--%>

                            <%--<div style="text-align: center; margin-bottom: 17px">--%>
                              <%--<span class="chart" data-percent="50" id="dataper2" >--%>
                                  <%--<span class="percent" ></span>--%>
                              <%--</span>--%>
                            <%--</div>--%>

                            <%--<h3 class="name_title">Disk</h3>--%>
                            <%--<p>Used ratio</p>--%>

                            <%--<div class="divider"></div>--%>
                            <%--<br/>--%>
                            <%--<p>The status is refreshed after 5 seconds</p>--%>

                        <%--</div>--%>
                    <%--</div>--%>
                <%--</div>--%>


                <!--</div>-->
                <!--<br />-->
                <!--<div class="row">-->
                <%--<div class="col-md-4 col-sm-4 col-xs-4">--%>
                    <%--<div class="x_panel">--%>
                        <%--<div class="x_title">--%>
                            <%--<h2>System State <small>Users</small></h2>--%>
                            <%--<ul class="nav navbar-right panel_toolbox">--%>
                                <%--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>--%>
                                <%--</li>--%>
                                <%--<li class="dropdown">--%>
                                    <%--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>--%>
                                    <%--<ul class="dropdown-menu" role="menu">--%>
                                        <%--<li><a href="#">Settings 1</a>--%>
                                        <%--</li>--%>
                                        <%--<li><a href="#">Settings 2</a>--%>
                                        <%--</li>--%>
                                    <%--</ul>--%>
                                <%--</li>--%>
                                <%--<li><a class="close-link"><i class="fa fa-close"></i></a>--%>
                                <%--</li>--%>
                            <%--</ul>--%>
                            <%--<div class="clearfix"></div>--%>
                        <%--</div>--%>
                        <%--<div class="x_content">--%>
                            <%--<p class="text-muted font-13 m-b-30">--%>
                                <%--DataTabTables has most features enabled by default, so all you need to do to use it with your own tables is to call the construction function: <code>$().DataTable();</code>--%>
                            <%--</p>--%>
                            <%--<table id="datatable" class="table table-striped table-bordered">--%>
                                <%--<thead>--%>
                                <%--<tr>--%>
                                    <%--<th>Name</th>--%>
                                    <%--<th>Position</th>--%>
                                <%--</tr>--%>
                                <%--</thead>--%>
                                <%--<tbody>--%>


                                <%--&lt;%&ndash;traverse all over the hashMap key&ndash;%&gt;--%>
                                <%--<%--%>
                                    <%--if(sys != null){--%>
                                        <%--Iterator iter = sys.getHashMap().entrySet().iterator();--%>
                                        <%--while (iter.hasNext()) {--%>
                                            <%--Map.Entry entry = (Map.Entry) iter.next();--%>
                                            <%--String key = (String) entry.getKey();--%>
                                            <%--String val = (String) entry.getValue();--%>
                                            <%--System.out.println("key "+ key);--%>
                                <%--%>--%>
                                <%--<tr>--%>
                                    <%--<th><%=key%></th>--%>
                                    <%--<th><%=val%></th>--%>
                                <%--</tr>--%>
                                <%--&lt;%&ndash;<script type="text/javascript">alert(<%=key%>);alert(<%=val%>);</script>>&ndash;%&gt;--%>
                                <%--<%--%>
                                        <%--}--%>

                                    <%--}--%>
                                <%--%>--%>
                                <%--</tbody>--%>
                            <%--</table>--%>
                        <%--</div>--%>
                    <%--</div>--%>
                <%--</div>--%>
            </div>
            <div class="clearfix"></div>
            <br>
            <%--<div class="row">--%>
                <%--&lt;%&ndash;<jsp:include page="tables_dynamic.html" flush="true"/><!--动态包含-->&ndash;%&gt;--%>
                <%--<%@include file="tables_dynamic.html"%><!--静态包含-->--%>
            <%--</div>--%>
            <div class="row" id="btnAll">
                    <div class="col-md-12 col-sm-12 col-xs-12">
                        <div class="x_panel">
                            <div class="x_title">
                                <h2>System Parameters</h2>
                                <%--<ul class="nav navbar-right panel_toolbox">--%>
                                    <%--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>--%>
                                    <%--</li>--%>
                                    <%--<li class="dropdown">--%>
                                        <%--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>--%>
                                        <%--<ul class="dropdown-menu" role="menu">--%>
                                            <%--<li><a href="#">Settings 1</a>--%>
                                            <%--</li>--%>
                                            <%--<li><a href="#">Settings 2</a>--%>
                                            <%--</li>--%>
                                        <%--</ul>--%>
                                    <%--</li>--%>
                                    <%--<li><a class="close-link"><i class="fa fa-close"></i></a>--%>
                                    <%--</li>--%>
                                <%--</ul>--%>
                                <div class="clearfix"></div>
                            </div>
                            <div class="x_content">
                                <%--<p class="text-muted font-13 m-b-30">--%>
                                    <%--DataTables has most features enabled by default, so all you need to do to use it with your own tables is to call the construction function: <code>$().DataTable();</code>--%>
                                <%--</p>--%>
                                <table id="datatable" class="table table-striped table-bordered">
                                    <thead>
                                    <tr>
                                        <th style="text-align: left">Name </th>
                                        <th style="text-align: left">Status</th>
                                        <%--<th>Office</th>--%>
                                        <%--<th>Age</th>--%>
                                        <%--<th>Start date</th>--%>
                                        <%--<th>Salary</th>--%>
                                    </tr>
                                    </thead>
                                    <tbody id="tableTbody">


                                    <%--<tr>--%>
                                        <%--<td>Tiger Nixon</td>--%>
                                        <%--<td>System Architect</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>61</td>--%>
                                        <%--<td>2011/04/25</td>--%>
                                        <%--<td>$320,800</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Garrett Winters</td>--%>
                                        <%--<td>Accountant</td>--%>
                                        <%--<td>Tokyo</td>--%>
                                        <%--<td>63</td>--%>
                                        <%--<td>2011/07/25</td>--%>
                                        <%--<td>$170,750</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Ashton Cox</td>--%>
                                        <%--<td>Junior Technical Author</td>--%>
                                        <%--<td>San Francisco</td>--%>
                                        <%--<td>66</td>--%>
                                        <%--<td>2009/01/12</td>--%>
                                        <%--<td>$86,000</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Cedric Kelly</td>--%>
                                        <%--<td>Senior Javascript Developer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>22</td>--%>
                                        <%--<td>2012/03/29</td>--%>
                                        <%--<td>$433,060</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Airi Satou</td>--%>
                                        <%--<td>Accountant</td>--%>
                                        <%--<td>Tokyo</td>--%>
                                        <%--<td>33</td>--%>
                                        <%--<td>2008/11/28</td>--%>
                                        <%--<td>$162,700</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Brielle Williamson</td>--%>
                                        <%--<td>Integration Specialist</td>--%>
                                        <%--<td>New York</td>--%>
                                        <%--<td>61</td>--%>
                                        <%--<td>2012/12/02</td>--%>
                                        <%--<td>$372,000</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Herrod Chandler</td>--%>
                                        <%--<td>Sales Assistant</td>--%>
                                        <%--<td>San Francisco</td>--%>
                                        <%--<td>59</td>--%>
                                        <%--<td>2012/08/06</td>--%>
                                        <%--<td>$137,500</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Rhona Davidson</td>--%>
                                        <%--<td>Integration Specialist</td>--%>
                                        <%--<td>Tokyo</td>--%>
                                        <%--<td>55</td>--%>
                                        <%--<td>2010/10/14</td>--%>
                                        <%--<td>$327,900</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Colleen Hurst</td>--%>
                                        <%--<td>Javascript Developer</td>--%>
                                        <%--<td>San Francisco</td>--%>
                                        <%--<td>39</td>--%>
                                        <%--<td>2009/09/15</td>--%>
                                        <%--<td>$205,500</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    <%--<tr>--%>
                                        <%--<td>Sonya Frost</td>--%>
                                        <%--<td>Software Engineer</td>--%>
                                        <%--<td>Edinburgh</td>--%>
                                        <%--<td>23</td>--%>
                                        <%--<td>2008/12/13</td>--%>
                                        <%--<td>$103,600</td>--%>
                                    <%--</tr>--%>
                                    </tbody>
                                </table>
                                <a id="sjzl"></a>&nbsp;
                                    <a id="btn0"></a>
                                    <input id="pageSize" type="hidden" size="1" maxlength="2" value="getDefaultValue()" style="width:25px;" />
                                    <div style="float: right">
                                    <%--<a> 条 </a> <a href="#" id="pageSizeSet">设置</a>&nbsp;--%>
                                    <a  href="#btnAll" id="btn1" style="margin-right: 20px;">Home</a>
                                    <a  href="#btnAll" id="btn2" >Previous</a>
                                    <a  href="#btnAll" id="btn3" style="margin-right: 20px;">Next</a>
                                    <a  href="#btnAll" id="btn4">Last</a>&nbsp;
                                    <%--<a>转到&nbsp;</a>--%>
                                    <%--<input id="changePage" type="text" size="1" maxlength="4"/>--%>
                                    <%--<a>页&nbsp;</a>--%>
                                    <%--<a  href="#btn1" id="btn5">Jump</a>--%>
                                </div>
                            </div>
                        </div>
                    </div>
            </div>

        </div>
        <!-- /page content -->

        <!-- footer content -->
        <footer>
            <div class="pull-right">
                <%--Gentelella - Bootstrap Admin Template by <a href="https://colorlib.com">Colorlib</a>--%>
                DITIR by ADSC
            </div>
            <div class="clearfix"></div>
        </footer>
        <!-- /footer content -->

        <%--<form action="../../clientTest" &lt;%&ndash;id="showDataForm" target="nm_iframe"&ndash;%&gt; &lt;%&ndash; onsubmit="return saveReport();&ndash;%&gt;>--%>
            <%--<input type="submit" name="query" value="Fresh" style="border:0px;width: 60px;float: right;background-color:#2a9fd6; ">--%>
        <%--</form>--%>


        <%--<%SystemState jsonstr = (SystemState) request.getSession().getAttribute("tupleList");%>--%>
        <%--<%String jsonStr = (String) request.getSession().getAttribute("tupleList");%>--%>
         <%if(jsonStr != null){
           %><input  id="json" value=<%=jsonStr%>>
        <%}%>
    </div>
</div>

<!-- Custom Theme Scripts -->
<script type="text/javascript">

    function a(){
        var arr = [1,2,3,2,1];
//        init_flot_chart(arr);
        $.ajax({
            type: 'GET',
            url:  "<%=path%>/clientTest",
            data: null ,
            dataType: "json",
            success:function(data) {
//                alert("success");
                var nowStr = data;
                var dataPercent = 80;
                var dataPercent2 = 60;
                var per = document.getElementById("dataper");
                $("#dataper").attr("data-percent",nowStr.ratio*100);
                $("#dataper").remove("data-percent");
//                alert(nowStr.ratio);
                var per2 = document.getElementById("dataper2");
                $("#dataper2").attr("data-percent", (1 - nowStr.availableDiskSpaceInGB / nowStr.totalDiskSpaceInGB) * 100);
//                $.per.setAttribute("data-percent","1");
//                per.dataset.percent= "11
//                alert(nowStr.throughput);
//                var nowStr = JSON.parse(ajax.responseText);
//                var table = document.getElementById("tableTbody");
//                var rowNum=table.rows.length;
//                if(rowNum>0) {
//                    for (i = 0; i < rowNum; i++) {
//                        table.deleteRow(i);
//                    }
//                }
////              alert(nowStr.hashMap.length);
//                for(var k in nowStr.hashMap){
//                    var newRow = table.insertRow(); //创建新行
//                    var newCell1 = newRow.insertCell(0); //创建新单元格
//                    newCell1.innerHTML = "<td>"+k+"</td>" ; //单元格内的内容
//                    newCell1.setAttribute("align","left"); //设置位置
//                    var newCell1 = newRow.insertCell(1); //创建新单元格
//                    newCell1.innerHTML =" <td>"+nowStr.hashMap[k]+"</td> "; //单元格内的内容
////                    alert(nowStr.hashMap[k]);
//                    newCell1.setAttribute("align","left"); //设置位置
//                }
//                var arr = [1,2,3,4,5];
//                init_flot_chart(nowStr.lastThroughput);
                $('canvas').remove();
                var cpu = parseFloat(nowStr.ratio * 100).toFixed(1);
                var idle = (100 - nowStr.ratio * 100).toFixed(1);
                var result = parseFloat(nowStr.availableDiskSpaceInGB).toFixed(1);
                var result2 = parseFloat(nowStr.totalDiskSpaceInGB).toFixed(1);
//                if( typeof ($.fn.easyPieChart) === 'undefined'){ return; }
//                console.log('init_EasyPieChart');
                $('.chart').easyPieChart({
                    easing: 'easeOutElastic',
                    delay: 3000,
                    barColor: '#26B99A',
                    trackColor: '#E6E9ED',
                    scaleColor: false,
                    lineWidth: 20,
                    trackWidth: 2,
                    lineCap: 'butt',
                    onStep: function(from, to, percent) {
                        var showPer = parseFloat(percent).toFixed(1);
                        $(this.el).find('.percent').text(showPer);
                        $('.show_per1').text(idle + "% idle");
                        $('.show_per2').text(result + "GB / " + result2 + "GB");
                    }
                });

//                init_EasyPieChart(result,result2);
                init_echarts(nowStr.lastThroughput);
            },
            error : function() {
                // view("异常！");
                // alert("failed！");
            }
        });

    }
    a();
    window.setInterval(a, 5000);
    //    $(function(){
    //           $('#showDataForm').submit();

//    var myName = new Array();
//    var jsonarray = document.getElementById("json").value;
//    var b=JSON.parse(jsonarray);//method 1
//    var jsonObject = eval("("+jsonarray+")");//method 1
    <%--var szJsonStr = "<%=request.getSession().getAttribute("tupleList")%>";--%>
    <%--var szJsonStr2 = '<s:property escapeJavaScript="false" escape="false" value="<%=request.getSession().getAttribute("tupleList")%>" />';--%>
    <%--var addVehicleArray = eval("("+szJsonStr+")");--%>
//    alert(aaddVehicleArray[0].vehicleType);
    <%--myName = "<%=request.getSession().getAttribute("tupleList")%>";--%>
//    if(myName == "null"){
//        myName = new Array();
//        myName.push(1);
//        myName.push(22);
//        init_flot_chart(myName);
////        alert(myName[1]);
//    }
//    else{
//        init_flot_chart(myName);
//    }
    <%--<%--%>
       <%--double[] test = new double[]{11,22};--%>
       <%--double[] user = null;--%>
       <%--SystemState sys = null;--%>
       <%--if (session.getAttribute("tupleList") != null) {--%>
         <%--user = (double [])request.getSession().getAttribute("tupleList");--%>
       <%--}--%>
       <%--else{--%>
           <%--user = new double[2];--%>
           <%--user[0] = 1;--%>
           <%--user[1] = 2;--%>
       <%--}--%>
<%--//           if(session.getAttribute("systemState") != null){--%>
<%--//               sys = (SystemState) session.getAttribute("systemState");--%>
<%--//           }--%>
    <%--%>--%>
    <%--//    })--%>

    <%--//      jQuery(function ($) {--%>

    <%--//      });--%>
    <%--<%if (session.getAttribute("tupleList") != null) {--%>
        <%--%>--%>
        <%--&lt;%&ndash;init_flot_chart(<%=user%>);alert("tuple");&ndash;%&gt;--%>
    <%--<%--%>
    <%--}--%>
    <%--else{--%>
        <%--%>--%>
    <%--&lt;%&ndash;init_flot_chart(<%=test%>);&ndash;%&gt;--%>
    <%--&lt;%&ndash;init_flot_chart(<%=user%>);&ndash;%&gt;--%>
       <%--<%--%>
    <%--}--%>
    <%--%>--%>
</script>
<script src="gentelella-master/build/js/custom.js"></script>
<script type="text/javascript">
    var num = 0;//tableNum
    function b(){
//        init_flot_chart(arr);
        $.ajax({
            type: 'GET',
            url:  "<%=path%>/clientTest",
            data: null ,
            dataType: "json",
            async: false,
            success:function(data) {
//                alert("success");
                var nowStr = data;
//                alert(nowStr.throughput);
//                var nowStr = JSON.parse(ajax.responseText);
//                                                    document.getElementsByClassName("chart").getAttribute('data-percent').innerHTML = dataPercent;
                var table = document.getElementById("tableTbody");
                var rowNum=table.rows.length;
                num = table.rows.length;
//                if(rowNum == 0){
//                    for(var k in nowStr.hashMap){
//                        var newRow = table.insertRow(); //创建新行
//                        var newCell1 = newRow.insertCell(0); //创建新单元格
//                        newCell1.innerHTML = "<td>"+k+"</td>" ; //单元格内的内容
//                        newCell1.setAttribute("align","center"); //设置位置
//                        var newCell1 = newRow.insertCell(1); //创建新单元格
//                        newCell1.innerHTML =" <td>"+nowStr.hashMap[k]+"</td> "; //单元格内的内容
////                    alert(nowStr.hashMap[k]);
//                        newCell1.setAttribute("align","center"); //设置位置
//                    }
//                }
//                else{
//                    var j = 0;
//                    for(var k in nowStr.hashMap){
////                        document.getElementById("tableTbody").rows[0].cells[0].innerText= "<td>"+k+"</td>" ;
////                        document.getElementById("tableTbody").rows[0].cells[0].innerText= "<td>"+nowStr.hashMap[k]+"</td>" ;
//                        if(document.getElementById("tableTbody").rows[j] == undefined){
////                            alert(rowNum);
//                            break;
//                        }
//                        document.getElementById("tableTbody").rows[j].cells[0].innerText= k;
//                        document.getElementById("tableTbody").rows[j].cells[1].innerText= nowStr.hashMap[k];
//                        document.write("<script src='../vendors/datatables.net/js/jquery.dataTables.min.js'><\/script>");
//                        j++;
//
////                        if(j >= rowNum){
////                            alert("length: "+j);
////                            break;
////                        }
//                    }
//
//                }
                for(var i=0;i<rowNum;i++)
                {
                    table.deleteRow(i);
                    rowNum=rowNum-1;
                    i=i-1;
                }
               for(var k in nowStr.treeMap){
                    var newRow = table.insertRow(); //创建新行
                    var newCell1 = newRow.insertCell(0); //创建新单元格
                    newCell1.innerHTML = "<td>"+k+"</td>" ; //单元格内的内容
                    newCell1.setAttribute("align","left"); //设置位置
                    var newCell1 = newRow.insertCell(1); //创建新单元格
                    newCell1.innerHTML =" <td>"+nowStr.treeMap[k]+"</td> "; //单元格内的内容
//                    alert(nowStr.hashMap[k]);
                    newCell1.setAttribute("align","left"); //设置位置
                }

                var pageSize = 10;    //每页显示的记录条数
                var curPage=0;        //当前页
                var lastPage;        //最后页
                var direct=0;        //方向
                var len;            //总行数
                var page;            //总页数
                var begin;
                var end;


                $(document).ready(function display(){
                    len =$("#datatable tr").length -1;    // 求这个表的总行数，剔除第一行介绍
                    page=len % pageSize==0 ? len/pageSize : Math.floor(len/pageSize)+1;//根据记录条数，计算页数
                    // alert("page==="+page);
                    curPage=1;    // 设置当前为第一页
                    displayPage(1);//显示第一页

                    document.getElementById("btn0").innerHTML="current " + curPage + "/" + page + " page ";    // 显示当前多少页
//                    document.getElementById("sjzl").innerHTML="数据总量 " + len + "";        // 显示数据量
                    document.getElementById("pageSize").value = pageSize;



                    $("#btn1").click(function firstPage(){    // 首页
                        curPage=1;
                        direct = 0;
                        displayPage();
                    });
                    $("#btn2").click(function frontPage(){    // 上一页
                        direct=-1;
                        displayPage();
                    });
                    $("#btn3").click(function nextPage(){    // 下一页
                        direct=1;
                        displayPage();
                    });
                    $("#btn4").click(function lastPage(){    // 尾页
                        curPage=page;
                        direct = 0;
                        displayPage();
                    });
                    $("#btn5").click(function changePage(){    // 转页
                        curPage=document.getElementById("changePage").value * 1;
                        if (!/^[1-9]\d*$/.test(curPage)) {
                            alert("请输入正整数");
                            return ;
                        }
                        if (curPage > page) {
                            alert("超出数据页面");
                            return ;
                        }
                        direct = 0;
                        displayPage();
                    });


                    $("#pageSizeSet").click(function setPageSize(){    // 设置每页显示多少条记录
                        pageSize = document.getElementById("pageSize").value;    //每页显示的记录条数
                        if (!/^[1-9]\d*$/.test(pageSize)) {
                            alert("请输入正整数");
                            return ;
                        }
                        len =$("#datatable tr").length - 1;
                        page=len % pageSize==0 ? len/pageSize : Math.floor(len/pageSize)+1;//根据记录条数，计算页数
                        curPage=1;        //当前页
                        direct=0;        //方向
//                        firstPage();
                    });
                });

                function displayPage(){
                    if(curPage <=1 && direct==-1){
                        direct=0;
//                        alert("已经是第一页了");
                        return;
                    } else if (curPage >= page && direct==1) {
                        direct=0;
//                        alert("已经是最后一页了");
                        return ;
                    }

                    lastPage = curPage;

                    // 修复当len=1时，curPage计算得0的bug
                    if (len > pageSize) {
                        curPage = ((curPage + direct + len) % len);
                    } else {
                        curPage = 1;
                    }


                    document.getElementById("btn0").innerHTML="current " + curPage + "/" + page + " page";        // 显示当前多少页

                    begin=(curPage-1)*pageSize + 1;// 起始记录号
                    end = begin + 1*pageSize - 1;    // 末尾记录号


                    if(end > len ) end=len;
                    $("#datatable tr").hide();    // 首先，设置这行为隐藏
                    $("#datatable tr").each(function(i){    // 然后，通过条件判断决定本行是否恢复显示
                        if((i>=begin && i<=end) || i==0 )//显示begin<=x<=end的记录
                            $(this).show();
                    });

                }

//                init_DataTables();
                <%--document.write("<script language=javascript src='/js/import.js'></script>");--%>
//            document.write("<script src='../vendors/datatables.net/js/jquery.dataTables.min.js?ver=1'><\/script>");
//            alert("hello");
//                var arr = [1,2,3,4,5];
            },
            error : function() {
                // view("异常！");
                alert("failed！");
            }
        });

    };
    b();
    window.setInterval(b, 5000);
//    alert("success");
    //                                        const myFunction = async function() {
    //                                            await b();
    //                                            const x = await ajax1()
    //                                            const y = await ajax2()
    //                                            //等待两个异步ajax请求同时执行完毕后打印出数据
    //                                            console.log(x, y)
    //                                        };
    //                                        myFunction();
</script>
</div>
</body>
</html>
