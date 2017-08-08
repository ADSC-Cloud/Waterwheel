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

    <title>Gentelella Alela! | </title>

    <!-- Bootstrap -->
    <link href="../vendors/bootstrap/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Font Awesome -->
    <link href="../vendors/font-awesome/css/font-awesome.min.css" rel="stylesheet">
    <!-- NProgress -->
    <link href="../vendors/nprogress/nprogress.css" rel="stylesheet">
    <!-- iCheck -->
    <link href="../vendors/iCheck/skins/flat/green.css" rel="stylesheet">

    <!-- bootstrap-progressbar -->
    <link href="../vendors/bootstrap-progressbar/css/bootstrap-progressbar-3.3.4.min.css" rel="stylesheet">
    <!-- JQVMap -->
    <link href="../vendors/jqvmap/dist/jqvmap.min.css" rel="stylesheet"/>
    <!-- bootstrap-daterangepicker -->
    <link href="../vendors/bootstrap-daterangepicker/daterangepicker.css" rel="stylesheet">

    <!-- Custom Theme Style -->
    <link href="../build/css/custom.min.css" rel="stylesheet">

    <%--<link rel="stylesheet" href="../../js/jquery-1.10.2.min.js">--%>
</head>

<body class="nav-md">

<div class="container body">
    <div class="main_container">
        <div class="col-md-3 left_col">
            <div class="left_col scroll-view">
                <div class="navbar nav_title" style="border: 0;">
                    <a href="index.html" class="site_title"><i class="fa fa-paw"></i> <span>Gentelella Alela!</span></a>
                </div>

                <div class="clearfix"></div>

                <!-- menu profile quick info -->
                <div class="profile clearfix">
                    <div class="profile_pic">
                        <img src="images/img.jpg" alt="..." class="img-circle profile_img">
                    </div>
                    <div class="profile_info">
                        <span>Welcome,</span>
                        <input type="button" value="获取当前时间" id="ajax">
                        <hr>
                        <span class="ajaxTest"></span>
                        <h2>John Doe</h2>
                    </div>
                </div>
                <!-- /menu profile quick info -->
                <script type="text/javascript">
                    //    var xmlhttp;
                    //    if (window.XMLHttpRequest)
                    //    {
                    //        //  IE7+, Firefox, Chrome, Opera, Safari 浏览器执行代码
                    //        xmlhttp=new XMLHttpRequest();
                    //    }

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
                        var method = "GET";
                        var url = "<%=path%>/clientTest";
                        ajax.open(method, url, true);
                        //4. 真正发送异步请求
                        /*
                         content表示发送请求的内容，如果无内容的话，使用null表示
                         如果有内容，写成key=value形成，例如：username=jack&password=123
                         */
                        var content = "111";
                        ajax.send(content);
                        //5. ajax对象监听服务器的响应
                        ajax.onreadystatechange = function() {
                            //如果ajax对象，已经完全接收到了响应，
                            if (ajax.readyState == 4) {
                                //如果响应正确
                                if (ajax.status == 200) {
                                    var nowStr = JSON.parse(ajax.responseText);
                                    var table = document.getElementById("datatable");
                                    var t1=document.getElementById("datatable");

                                    var rowNum=t1.rows.length;
                                    if(rowNum>0) {
                                        for (i = 0; i < rowNum; i++) {
                                            t1.deleteRow(i);
                                            rowNum = rowNum - 1;
                                            i = i - 1;
                                        }
                                    }
                                    for(var k in nowStr.hashMap){
                                        var newRow = table.insertRow(); //创建新行
                                        var newCell1 = newRow.insertCell(0); //创建新单元格
                                        newCell1.innerHTML = k ; //单元格内的内容
                                        newCell1.setAttribute("align","center"); //设置位置
                                        var newCell1 = newRow.insertCell(1); //创建新单元格
                                        newCell1.innerHTML = nowStr.hashMap[k] ; //单元格内的内容
                                        newCell1.setAttribute("align","center"); //设置位置
                                    }
                                        init_flot_chart(nowStr);
                                    //将获取到的时间放在span标签内
                                    //定位span标签
//                                    var spanElement = document.getElementsByTagName("span")[0];
                                    //将nowStr放当span标签内
//                                    spanElement.innerHTML = nowStr;
                                }

                            }

                        };

                    };
                    chart();
                    window.setInterval(chart, 2000);
                    <%JSONObject jsonStr = (JSONObject)request.getSession().getAttribute("tupleList");%>
                    <%SystemState sys = (SystemState)request.getSession().getAttribute("systemState");%>
                </script>
                <br />

                <!-- sidebar menu -->
                <div id="sidebar-menu" class="main_menu_side hidden-print main_menu">
                    <div class="menu_section">
                        <h3>General</h3>
                        <ul class="nav side-menu">
                            <li><a><i class="fa fa-home"></i> Home <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="index.html">Dashboard</a></li>
                                    <li><a href="index2.html">Dashboard2</a></li>
                                    <li><a href="index3.html">Dashboard3</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-edit"></i> Forms <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="form.html">General Form</a></li>
                                    <li><a href="form_advanced.html">Advanced Components</a></li>
                                    <li><a href="form_validation.html">Form Validation</a></li>
                                    <li><a href="form_wizards.html">Form Wizard</a></li>
                                    <li><a href="form_upload.html">Form Upload</a></li>
                                    <li><a href="form_buttons.html">Form Buttons</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-desktop"></i> UI Elements <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="general_elements.html">General Elements</a></li>
                                    <li><a href="media_gallery.html">Media Gallery</a></li>
                                    <li><a href="typography.html">Typography</a></li>
                                    <li><a href="icons.html">Icons</a></li>
                                    <li><a href="glyphicons.html">Glyphicons</a></li>
                                    <li><a href="widgets.html">Widgets</a></li>
                                    <li><a href="invoice.html">Invoice</a></li>
                                    <li><a href="inbox.html">Inbox</a></li>
                                    <li><a href="calendar.html">Calendar</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-table"></i> Tables <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="tables.html">Tables</a></li>
                                    <li><a href="tables_dynamic.html">Table Dynamic</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-bar-chart-o"></i> Data Presentation <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="chartjs.html">Chart JS</a></li>
                                    <li><a href="chartjs2.html">Chart JS2</a></li>
                                    <li><a href="morisjs.html">Moris JS</a></li>
                                    <li><a href="echarts.html">ECharts</a></li>
                                    <li><a href="other_charts.html">Other Charts</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-clone"></i>Layouts <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="fixed_sidebar.html">Fixed Sidebar</a></li>
                                    <li><a href="fixed_footer.html">Fixed Footer</a></li>
                                </ul>
                            </li>
                        </ul>
                    </div>
                    <div class="menu_section">
                        <h3>Live On</h3>
                        <ul class="nav side-menu">
                            <li><a><i class="fa fa-bug"></i> Additional Pages <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="e_commerce.html">E-commerce</a></li>
                                    <li><a href="projects.html">Projects</a></li>
                                    <li><a href="project_detail.html">Project Detail</a></li>
                                    <li><a href="contacts.html">Contacts</a></li>
                                    <li><a href="profile.html">Profile</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-windows"></i> Extras <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="page_403.html">403 Error</a></li>
                                    <li><a href="page_404.html">404 Error</a></li>
                                    <li><a href="page_500.html">500 Error</a></li>
                                    <li><a href="plain_page.html">Plain Page</a></li>
                                    <li><a href="login.html">Login Page</a></li>
                                    <li><a href="pricing_tables.html">Pricing Tables</a></li>
                                </ul>
                            </li>
                            <li><a><i class="fa fa-sitemap"></i> Multilevel Menu <span class="fa fa-chevron-down"></span></a>
                                <ul class="nav child_menu">
                                    <li><a href="#level1_1">Level One</a>
                                    <li><a>Level One<span class="fa fa-chevron-down"></span></a>
                                        <ul class="nav child_menu">
                                            <li class="sub_menu"><a href="level2.html">Level Two</a>
                                            </li>
                                            <li><a href="#level2_1">Level Two</a>
                                            </li>
                                            <li><a href="#level2_2">Level Two</a>
                                            </li>
                                        </ul>
                                    </li>
                                    <li><a href="#level1_2">Level One</a>
                                    </li>
                                </ul>
                            </li>
                            <li><a href="javascript:void(0)"><i class="fa fa-laptop"></i> Landing Page <span class="label label-success pull-right">Coming Soon</span></a></li>
                        </ul>
                    </div>

                </div>
                <!-- /sidebar menu -->

                <!-- /menu footer buttons -->
                <div class="sidebar-footer hidden-small">
                    <a data-toggle="tooltip" data-placement="top" title="Settings">
                        <span class="glyphicon glyphicon-cog" aria-hidden="true"></span>
                    </a>
                    <a data-toggle="tooltip" data-placement="top" title="FullScreen">
                        <span class="glyphicon glyphicon-fullscreen" aria-hidden="true"></span>
                    </a>
                    <a data-toggle="tooltip" data-placement="top" title="Lock">
                        <span class="glyphicon glyphicon-eye-close" aria-hidden="true"></span>
                    </a>
                    <a data-toggle="tooltip" data-placement="top" title="Logout" href="login.html">
                        <span class="glyphicon glyphicon-off" aria-hidden="true"></span>
                    </a>
                </div>
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

                    <ul class="nav navbar-nav navbar-right">
                        <li class="">
                            <a href="javascript:;" class="user-profile dropdown-toggle" data-toggle="dropdown" aria-expanded="false">
                                <img src="images/img.jpg" alt="">John Doe
                                <span class=" fa fa-angle-down"></span>
                            </a>
                            <ul class="dropdown-menu dropdown-usermenu pull-right">
                                <li><a href="javascript:;"> Profile</a></li>
                                <li>
                                    <a href="javascript:;">
                                        <span class="badge bg-red pull-right">50%</span>
                                        <span>Settings</span>
                                    </a>
                                </li>
                                <li><a href="javascript:;">Help</a></li>
                                <li><a href="login.html"><i class="fa fa-sign-out pull-right"></i> Log Out</a></li>
                            </ul>
                        </li>

                        <li role="presentation" class="dropdown">
                            <a href="javascript:;" class="dropdown-toggle info-number" data-toggle="dropdown" aria-expanded="false">
                                <i class="fa fa-envelope-o"></i>
                                <span class="badge bg-green">6</span>
                            </a>
                            <ul id="menu1" class="dropdown-menu list-unstyled msg_list" role="menu">
                                <li>
                                    <a>
                                        <span class="image"><img src="images/img.jpg" alt="Profile Image" /></span>
                                        <span>
                          <span>John Smith</span>
                          <span class="time">3 mins ago</span>
                        </span>
                                        <span class="message">
                          Film festivals used to be do-or-die moments for movie makers. They were where...
                        </span>
                                    </a>
                                </li>
                                <li>
                                    <a>
                                        <span class="image"><img src="images/img.jpg" alt="Profile Image" /></span>
                                        <span>
                          <span>John Smith</span>
                          <span class="time">3 mins ago</span>
                        </span>
                                        <span class="message">
                          Film festivals used to be do-or-die moments for movie makers. They were where...
                        </span>
                                    </a>
                                </li>
                                <li>
                                    <a>
                                        <span class="image"><img src="images/img.jpg" alt="Profile Image" /></span>
                                        <span>
                          <span>John Smith</span>
                          <span class="time">3 mins ago</span>
                        </span>
                                        <span class="message">
                          Film festivals used to be do-or-die moments for movie makers. They were where...
                        </span>
                                    </a>
                                </li>
                                <li>
                                    <a>
                                        <span class="image"><img src="images/img.jpg" alt="Profile Image" /></span>
                                        <span>
                          <span>John Smith</span>
                          <span class="time">3 mins ago</span>
                        </span>
                                        <span class="message">
                          Film festivals used to be do-or-die moments for movie makers. They were where...
                        </span>
                                    </a>
                                </li>
                                <li>
                                    <div class="text-center">
                                        <a>
                                            <strong>See All Alerts</strong>
                                            <i class="fa fa-angle-right"></i>
                                        </a>
                                    </div>
                                </li>
                            </ul>
                        </li>
                    </ul>
                </nav>
            </div>
        </div>
        <!-- /top navigation -->

        <!-- page content -->
        <div class="right_col" role="main">
            <!-- top tiles -->
            <div class="row tile_count">
                <div class="col-md-12 col-sm-12 col-xs-12 tile_stats_count">
                    <span class="count_top"><i class="fa fa-user"></i> Total Users</span>
                    <h1>Waterwheel  <small>A distributed system for high-rate data indexing and real-time querying</small></h1>
                    <span class="count_bottom"><i class="green">4% </i> From last Week</span>
                </div>
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
                <div class="col-md-8 col-sm-8 col-xs-8">
                    <div class="dashboard_graph">

                        <div class="row x_title">
                            <div class="col-md-8">
                                <h3>Overall Insertion Throughput <small>tuple/s</small></h3>
                            </div>
                            <div class="col-md-8">
                                <!--<div id="reportrange" class="pull-right" style="background: #fff; cursor: pointer; padding: 5px 10px; border: 1px solid #ccc">-->
                                <!--<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>-->
                                <!--<span>December 30, 2014 - January 28, 2015</span> <b class="caret"></b>-->
                                <!--</div>-->
                            </div>
                        </div>

                        <div class="col-md-12 col-sm-12 col-xs-12">
                            <div id="chart_plot_03" class="demo-placeholder"></div>
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

                <!--</div>-->
                <!--<br />-->
                <!--<div class="row">-->
                <div class="col-md-4 col-sm-4 col-xs-4">
                    <div class="x_panel">
                        <div class="x_title">
                            <h2>System State <small>Users</small></h2>
                            <ul class="nav navbar-right panel_toolbox">
                                <li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>
                                </li>
                                <li class="dropdown">
                                    <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>
                                    <ul class="dropdown-menu" role="menu">
                                        <li><a href="#">Settings 1</a>
                                        </li>
                                        <li><a href="#">Settings 2</a>
                                        </li>
                                    </ul>
                                </li>
                                <li><a class="close-link"><i class="fa fa-close"></i></a>
                                </li>
                            </ul>
                            <div class="clearfix"></div>
                        </div>
                        <div class="x_content">
                            <p class="text-muted font-13 m-b-30">
                                DataTables has most features enabled by default, so all you need to do to use it with your own tables is to call the construction function: <code>$().DataTable();</code>
                            </p>
                            <table id="datatable" class="table table-striped table-bordered">
                                <thead>
                                <tr>
                                    <th>Name</th>
                                    <th>Position</th>
                                </tr>
                                </thead>
                                <tbody>


                                <%--traverse all over the hashMap key--%>
                                <%
                                    if(sys != null){
                                        Iterator iter = sys.getHashMap().entrySet().iterator();
                                        while (iter.hasNext()) {
                                            Map.Entry entry = (Map.Entry) iter.next();
                                            String key = (String) entry.getKey();
                                            String val = (String) entry.getValue();
                                            System.out.println("key "+ key);
                                %>
                                <tr>
                                    <th><%=key%></th>
                                    <th><%=val%></th>
                                </tr>
                                <%--<script type="text/javascript">alert(<%=key%>);alert(<%=val%>);</script>>--%>
                                <%
                                        }

                                    }
                                %>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
            <!--<div class="row">-->


            <!--<div class="col-md-4 col-sm-4 col-xs-12">-->
            <!--<div class="x_panel tile fixed_height_320">-->
            <!--<div class="x_title">-->
            <!--<h2>App Versions</h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->
            <!--<h4>App Usage across versions</h4>-->
            <!--<div class="widget_summary">-->
            <!--<div class="w_left w_25">-->
            <!--<span>0.1.5.2</span>-->
            <!--</div>-->
            <!--<div class="w_center w_55">-->
            <!--<div class="progress">-->
            <!--<div class="progress-bar bg-green" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 66%;">-->
            <!--<span class="sr-only">60% Complete</span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="w_right w_20">-->
            <!--<span>123k</span>-->
            <!--</div>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->

            <!--<div class="widget_summary">-->
            <!--<div class="w_left w_25">-->
            <!--<span>0.1.5.3</span>-->
            <!--</div>-->
            <!--<div class="w_center w_55">-->
            <!--<div class="progress">-->
            <!--<div class="progress-bar bg-green" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 45%;">-->
            <!--<span class="sr-only">60% Complete</span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="w_right w_20">-->
            <!--<span>53k</span>-->
            <!--</div>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="widget_summary">-->
            <!--<div class="w_left w_25">-->
            <!--<span>0.1.5.4</span>-->
            <!--</div>-->
            <!--<div class="w_center w_55">-->
            <!--<div class="progress">-->
            <!--<div class="progress-bar bg-green" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 25%;">-->
            <!--<span class="sr-only">60% Complete</span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="w_right w_20">-->
            <!--<span>23k</span>-->
            <!--</div>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="widget_summary">-->
            <!--<div class="w_left w_25">-->
            <!--<span>0.1.5.5</span>-->
            <!--</div>-->
            <!--<div class="w_center w_55">-->
            <!--<div class="progress">-->
            <!--<div class="progress-bar bg-green" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 5%;">-->
            <!--<span class="sr-only">60% Complete</span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="w_right w_20">-->
            <!--<span>3k</span>-->
            <!--</div>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="widget_summary">-->
            <!--<div class="w_left w_25">-->
            <!--<span>0.1.5.6</span>-->
            <!--</div>-->
            <!--<div class="w_center w_55">-->
            <!--<div class="progress">-->
            <!--<div class="progress-bar bg-green" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 2%;">-->
            <!--<span class="sr-only">60% Complete</span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="w_right w_20">-->
            <!--<span>1k</span>-->
            <!--</div>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->

            <!--</div>-->
            <!--</div>-->
            <!--</div>-->

            <!--<div class="col-md-4 col-sm-4 col-xs-12">-->
            <!--<div class="x_panel tile fixed_height_320 overflow_hidden">-->
            <!--<div class="x_title">-->
            <!--<h2>Device Usage</h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->
            <!--<table class="" style="width:100%">-->
            <!--<tr>-->
            <!--<th style="width:37%;">-->
            <!--<p>Top 5</p>-->
            <!--</th>-->
            <!--<th>-->
            <!--<div class="col-lg-7 col-md-7 col-sm-7 col-xs-7">-->
            <!--<p class="">Device</p>-->
            <!--</div>-->
            <!--<div class="col-lg-5 col-md-5 col-sm-5 col-xs-5">-->
            <!--<p class="">Progress</p>-->
            <!--</div>-->
            <!--</th>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>-->
            <!--<canvas class="canvasDoughnut" height="140" width="140" style="margin: 15px 10px 10px 0"></canvas>-->
            <!--</td>-->
            <!--<td>-->
            <!--<table class="tile_info">-->
            <!--<tr>-->
            <!--<td>-->
            <!--<p><i class="fa fa-square blue"></i>IOS </p>-->
            <!--</td>-->
            <!--<td>30%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>-->
            <!--<p><i class="fa fa-square green"></i>Android </p>-->
            <!--</td>-->
            <!--<td>10%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>-->
            <!--<p><i class="fa fa-square purple"></i>Blackberry </p>-->
            <!--</td>-->
            <!--<td>20%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>-->
            <!--<p><i class="fa fa-square aero"></i>Symbian </p>-->
            <!--</td>-->
            <!--<td>15%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>-->
            <!--<p><i class="fa fa-square red"></i>Others </p>-->
            <!--</td>-->
            <!--<td>30%</td>-->
            <!--</tr>-->
            <!--</table>-->
            <!--</td>-->
            <!--</tr>-->
            <!--</table>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->


            <!--<div class="col-md-4 col-sm-4 col-xs-12">-->
            <!--<div class="x_panel tile fixed_height_320">-->
            <!--<div class="x_title">-->
            <!--<h2>Quick Settings</h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->
            <!--<div class="dashboard-widget-content">-->
            <!--<ul class="quick-list">-->
            <!--<li><i class="fa fa-calendar-o"></i><a href="#">Settings</a>-->
            <!--</li>-->
            <!--<li><i class="fa fa-bars"></i><a href="#">Subscription</a>-->
            <!--</li>-->
            <!--<li><i class="fa fa-bar-chart"></i><a href="#">Auto Renewal</a> </li>-->
            <!--<li><i class="fa fa-line-chart"></i><a href="#">Achievements</a>-->
            <!--</li>-->
            <!--<li><i class="fa fa-bar-chart"></i><a href="#">Auto Renewal</a> </li>-->
            <!--<li><i class="fa fa-line-chart"></i><a href="#">Achievements</a>-->
            <!--</li>-->
            <!--<li><i class="fa fa-area-chart"></i><a href="#">Logout</a>-->
            <!--</li>-->
            <!--</ul>-->

            <!--<div class="sidebar-widget">-->
            <!--<h4>Profile Completion</h4>-->
            <!--<canvas width="150" height="80" id="chart_gauge_01" class="" style="width: 160px; height: 100px;"></canvas>-->
            <!--<div class="goal-wrapper">-->
            <!--<span id="gauge-text" class="gauge-value pull-left">0</span>-->
            <!--<span class="gauge-value pull-left">%</span>-->
            <!--<span id="goal-text" class="goal-value pull-right">100%</span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->

            <!--</div>-->


            <!--<div class="row">-->
            <!--<div class="col-md-4 col-sm-4 col-xs-12">-->
            <!--<div class="x_panel">-->
            <!--<div class="x_title">-->
            <!--<h2>Recent Activities <small>Sessions</small></h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->
            <!--<div class="dashboard-widget-content">-->

            <!--<ul class="list-unstyled timeline widget">-->
            <!--<li>-->
            <!--<div class="block">-->
            <!--<div class="block_content">-->
            <!--<h2 class="title">-->
            <!--<a>Who Needs Sundance When You’ve Got&nbsp;Crowdfunding?</a>-->
            <!--</h2>-->
            <!--<div class="byline">-->
            <!--<span>13 hours ago</span> by <a>Jane Smith</a>-->
            <!--</div>-->
            <!--<p class="excerpt">Film festivals used to be do-or-die moments for movie makers. They were where you met the producers that could fund your project, and if the buyers liked your flick, they’d pay to Fast-forward and… <a>Read&nbsp;More</a>-->
            <!--</p>-->
            <!--</div>-->
            <!--</div>-->
            <!--</li>-->
            <!--<li>-->
            <!--<div class="block">-->
            <!--<div class="block_content">-->
            <!--<h2 class="title">-->
            <!--<a>Who Needs Sundance When You’ve Got&nbsp;Crowdfunding?</a>-->
            <!--</h2>-->
            <!--<div class="byline">-->
            <!--<span>13 hours ago</span> by <a>Jane Smith</a>-->
            <!--</div>-->
            <!--<p class="excerpt">Film festivals used to be do-or-die moments for movie makers. They were where you met the producers that could fund your project, and if the buyers liked your flick, they’d pay to Fast-forward and… <a>Read&nbsp;More</a>-->
            <!--</p>-->
            <!--</div>-->
            <!--</div>-->
            <!--</li>-->
            <!--<li>-->
            <!--<div class="block">-->
            <!--<div class="block_content">-->
            <!--<h2 class="title">-->
            <!--<a>Who Needs Sundance When You’ve Got&nbsp;Crowdfunding?</a>-->
            <!--</h2>-->
            <!--<div class="byline">-->
            <!--<span>13 hours ago</span> by <a>Jane Smith</a>-->
            <!--</div>-->
            <!--<p class="excerpt">Film festivals used to be do-or-die moments for movie makers. They were where you met the producers that could fund your project, and if the buyers liked your flick, they’d pay to Fast-forward and… <a>Read&nbsp;More</a>-->
            <!--</p>-->
            <!--</div>-->
            <!--</div>-->
            <!--</li>-->
            <!--<li>-->
            <!--<div class="block">-->
            <!--<div class="block_content">-->
            <!--<h2 class="title">-->
            <!--<a>Who Needs Sundance When You’ve Got&nbsp;Crowdfunding?</a>-->
            <!--</h2>-->
            <!--<div class="byline">-->
            <!--<span>13 hours ago</span> by <a>Jane Smith</a>-->
            <!--</div>-->
            <!--<p class="excerpt">Film festivals used to be do-or-die moments for movie makers. They were where you met the producers that could fund your project, and if the buyers liked your flick, they’d pay to Fast-forward and… <a>Read&nbsp;More</a>-->
            <!--</p>-->
            <!--</div>-->
            <!--</div>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->


            <!--<div class="col-md-8 col-sm-8 col-xs-12">-->



            <!--<div class="row">-->

            <!--<div class="col-md-12 col-sm-12 col-xs-12">-->
            <!--<div class="x_panel">-->
            <!--<div class="x_title">-->
            <!--<h2>Visitors location <small>geo-presentation</small></h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->
            <!--<div class="dashboard-widget-content">-->
            <!--<div class="col-md-4 hidden-small">-->
            <!--<h2 class="line_30">125.7k Views from 60 countries</h2>-->

            <!--<table class="countries_list">-->
            <!--<tbody>-->
            <!--<tr>-->
            <!--<td>United States</td>-->
            <!--<td class="fs15 fw700 text-right">33%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>France</td>-->
            <!--<td class="fs15 fw700 text-right">27%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>Germany</td>-->
            <!--<td class="fs15 fw700 text-right">16%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>Spain</td>-->
            <!--<td class="fs15 fw700 text-right">11%</td>-->
            <!--</tr>-->
            <!--<tr>-->
            <!--<td>Britain</td>-->
            <!--<td class="fs15 fw700 text-right">10%</td>-->
            <!--</tr>-->
            <!--</tbody>-->
            <!--</table>-->
            <!--</div>-->
            <!--<div id="world-map-gdp" class="col-md-8 col-sm-12 col-xs-12" style="height:230px;"></div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->

            <!--</div>-->
            <!--<div class="row">-->


            <!--&lt;!&ndash; Start to do list &ndash;&gt;-->
            <!--<div class="col-md-6 col-sm-6 col-xs-12">-->
            <!--<div class="x_panel">-->
            <!--<div class="x_title">-->
            <!--<h2>To Do List <small>Sample tasks</small></h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->

            <!--<div class="">-->
            <!--<ul class="to_do">-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Schedule meeting with new client </p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Create email address for new intern</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Have IT fix the network printer</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Copy backups to offsite location</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Food truck fixie locavors mcsweeney</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Food truck fixie locavors mcsweeney</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Create email address for new intern</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Have IT fix the network printer</p>-->
            <!--</li>-->
            <!--<li>-->
            <!--<p>-->
            <!--<input type="checkbox" class="flat"> Copy backups to offsite location</p>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--&lt;!&ndash; End to do list &ndash;&gt;-->
            <!---->
            <!--&lt;!&ndash; start of weather widget &ndash;&gt;-->
            <!--<div class="col-md-6 col-sm-6 col-xs-12">-->
            <!--<div class="x_panel">-->
            <!--<div class="x_title">-->
            <!--<h2>Daily active users <small>Sessions</small></h2>-->
            <!--<ul class="nav navbar-right panel_toolbox">-->
            <!--<li><a class="collapse-link"><i class="fa fa-chevron-up"></i></a>-->
            <!--</li>-->
            <!--<li class="dropdown">-->
            <!--<a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"><i class="fa fa-wrench"></i></a>-->
            <!--<ul class="dropdown-menu" role="menu">-->
            <!--<li><a href="#">Settings 1</a>-->
            <!--</li>-->
            <!--<li><a href="#">Settings 2</a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--</li>-->
            <!--<li><a class="close-link"><i class="fa fa-close"></i></a>-->
            <!--</li>-->
            <!--</ul>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--<div class="x_content">-->
            <!--<div class="row">-->
            <!--<div class="col-sm-12">-->
            <!--<div class="temperature"><b>Monday</b>, 07:30 AM-->
            <!--<span>F</span>-->
            <!--<span><b>C</b></span>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="row">-->
            <!--<div class="col-sm-4">-->
            <!--<div class="weather-icon">-->
            <!--<canvas height="84" width="84" id="partly-cloudy-day"></canvas>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-8">-->
            <!--<div class="weather-text">-->
            <!--<h2>Texas <br><i>Partly Cloudy Day</i></h2>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-12">-->
            <!--<div class="weather-text pull-right">-->
            <!--<h3 class="degrees">23</h3>-->
            <!--</div>-->
            <!--</div>-->

            <!--<div class="clearfix"></div>-->

            <!--<div class="row weather-days">-->
            <!--<div class="col-sm-2">-->
            <!--<div class="daily-weather">-->
            <!--<h2 class="day">Mon</h2>-->
            <!--<h3 class="degrees">25</h3>-->
            <!--<canvas id="clear-day" width="32" height="32"></canvas>-->
            <!--<h5>15 <i>km/h</i></h5>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-2">-->
            <!--<div class="daily-weather">-->
            <!--<h2 class="day">Tue</h2>-->
            <!--<h3 class="degrees">25</h3>-->
            <!--<canvas height="32" width="32" id="rain"></canvas>-->
            <!--<h5>12 <i>km/h</i></h5>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-2">-->
            <!--<div class="daily-weather">-->
            <!--<h2 class="day">Wed</h2>-->
            <!--<h3 class="degrees">27</h3>-->
            <!--<canvas height="32" width="32" id="snow"></canvas>-->
            <!--<h5>14 <i>km/h</i></h5>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-2">-->
            <!--<div class="daily-weather">-->
            <!--<h2 class="day">Thu</h2>-->
            <!--<h3 class="degrees">28</h3>-->
            <!--<canvas height="32" width="32" id="sleet"></canvas>-->
            <!--<h5>15 <i>km/h</i></h5>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-2">-->
            <!--<div class="daily-weather">-->
            <!--<h2 class="day">Fri</h2>-->
            <!--<h3 class="degrees">28</h3>-->
            <!--<canvas height="32" width="32" id="wind"></canvas>-->
            <!--<h5>11 <i>km/h</i></h5>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="col-sm-2">-->
            <!--<div class="daily-weather">-->
            <!--<h2 class="day">Sat</h2>-->
            <!--<h3 class="degrees">26</h3>-->
            <!--<canvas height="32" width="32" id="cloudy"></canvas>-->
            <!--<h5>10 <i>km/h</i></h5>-->
            <!--</div>-->
            <!--</div>-->
            <!--<div class="clearfix"></div>-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->

            <!--</div>-->
            <!--&lt;!&ndash; end of weather widget &ndash;&gt;-->
            <!--</div>-->
            <!--</div>-->
            <!--</div>-->
        </div>
        <!-- /page content -->

        <!-- footer content -->
        <footer>
            <div class="pull-right">
                Gentelella - Bootstrap Admin Template by <a href="https://colorlib.com">Colorlib</a>
            </div>
            <div class="clearfix"></div>
        </footer>
        <!-- /footer content -->

        <form action="../../clientTest" <%--id="showDataForm" target="nm_iframe"--%> <%-- onsubmit="return saveReport();--%>>
            <input type="submit" name="query" value="Fresh" style="border:0px;width: 60px;float: right;background-color:#2a9fd6; ">
        </form>


        <%--<%SystemState jsonstr = (SystemState) request.getSession().getAttribute("tupleList");%>--%>
        <%--<%String jsonStr = (String) request.getSession().getAttribute("tupleList");%>--%>
         <%if(jsonStr != null){
           %><input  id="json" value=<%=jsonStr%>>
        <%}%>
    </div>
</div>

<!-- jQuery -->
<script src="../vendors/jquery/dist/jquery.min.js"></script>
<!-- Bootstrap -->
<script src="../vendors/bootstrap/dist/js/bootstrap.min.js"></script>
<!-- FastClick -->
<script src="../vendors/fastclick/lib/fastclick.js"></script>
<!-- NProgress -->
<script src="../vendors/nprogress/nprogress.js"></script>
<!-- Chart.js -->
<script src="../vendors/Chart.js/dist/Chart.min.js"></script>
<!-- gauge.js -->
<script src="../vendors/gauge.js/dist/gauge.min.js"></script>
<!-- bootstrap-progressbar -->
<script src="../vendors/bootstrap-progressbar/bootstrap-progressbar.min.js"></script>
<!-- iCheck -->
<script src="../vendors/iCheck/icheck.min.js"></script>
<!-- Skycons -->
<script src="../vendors/skycons/skycons.js"></script>
<!-- Flot -->
<script src="../vendors/Flot/jquery.flot.js"></script>
<script src="../vendors/Flot/jquery.flot.pie.js"></script>
<script src="../vendors/Flot/jquery.flot.time.js"></script>
<script src="../vendors/Flot/jquery.flot.stack.js"></script>
<script src="../vendors/Flot/jquery.flot.resize.js"></script>
<!-- Flot plugins -->
<script src="../vendors/flot.orderbars/js/jquery.flot.orderBars.js"></script>
<script src="../vendors/flot-spline/js/jquery.flot.spline.min.js"></script>
<script src="../vendors/flot.curvedlines/curvedLines.js"></script>
<!-- DateJS -->
<script src="../vendors/DateJS/build/date.js"></script>
<!-- JQVMap -->
<script src="../vendors/jqvmap/dist/jquery.vmap.js"></script>
<script src="../vendors/jqvmap/dist/maps/jquery.vmap.world.js"></script>
<script src="../vendors/jqvmap/examples/js/jquery.vmap.sampledata.js"></script>
<!-- bootstrap-daterangepicker -->
<script src="../vendors/moment/min/moment.min.js"></script>
<script src="../vendors/bootstrap-daterangepicker/daterangepicker.js"></script>

<!-- Custom Theme Scripts -->
<script src="../build/js/custom.js"></script>
<script type="text/javascript">


    //    $(function(){
    //           $('#showDataForm').submit();

//    var myName = new Array();
    var jsonarray = document.getElementById("json").value;
    var b=JSON.parse(jsonarray);//method 1
    var jsonObject = eval("("+jsonarray+")");//method 1
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
</div>
</body>
</html>
