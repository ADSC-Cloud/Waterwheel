<%--
  Created by IntelliJ IDEA.
  User: billlin
  Date: 2017/7/25
  Time: 上午10:51
  To change this template use File | Settings | File Templates.
--%>
<%--<%@page import="ui.clientTest"%>--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="indexingTopology.*" %>
<%@ page import="javax.servlet.ServletRequest" %>
<%
  String path = request.getContextPath();
  String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<base href="<%=basePath%>">
<%--<%@ page import="org.apache.storm.topology.base.baseRichBolt" %>--%>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dashboard - Dark Admin</title>

  <link rel="stylesheet" type="text/css" href="bootstrap/css/bootstrap.min.css" />
  <link rel="stylesheet" type="text/css" href="font-awesome/css/font-awesome.min.css" />
  <link rel="stylesheet" type="text/css" href="css/local.css" />

  <script type="text/javascript" src="js/jquery-1.10.2.min.js"></script>
  <script type="text/javascript" src="bootstrap/js/bootstrap.min.js"></script>

  <!-- you need to include the shieldui css and js assets in order for the charts to work -->
  <link rel="stylesheet" type="text/css" href="http://www.shieldui.com/shared/components/latest/css/light-bootstrap/all.min.css" />
  <link id="gridcss" rel="stylesheet" type="text/css" href="http://www.shieldui.com/shared/components/latest/css/dark-bootstrap/all.min.css" />

  <script type="text/javascript" src="http://www.shieldui.com/shared/components/latest/js/shieldui-all.min.js"></script>
  <script type="text/javascript" src="http://www.prepbootstrap.com/Content/js/gridData.js"></script>
</head>
<body>
<script type="text/javascript">
    $(function(){
        <%
           double a = 0.0;
           double b = 0.0;
           double[] user = null;
           if (session.getAttribute("tupleList") != null) {
           user = (double [])request.getSession().getAttribute("tupleList");
           a = user[1];
           b = user[2];
           }
        %>
        <%--alert(<%=a%>);--%>

    })

</script>
<div id="wrapper">
  <nav class="navbar navbar-inverse navbar-fixed-top" role="navigation">
    <div class="navbar-header">
      <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-ex1-collapse">
        <span class="sr-only">Toggle navigation</span>
        <span class="icon-bar"></span>
        <span class="icon-bar"></span>
        <span class="icon-bar"></span>
      </button>
      <a class="navbar-brand" href="css/index.html">Waterwheel</a>
    </div>
    <div class="collapse navbar-collapse navbar-ex1-collapse">
      <ul id="active" class="nav navbar-nav side-nav">
        <li class="selected"><a href="css/index.html"><i class="fa fa-bullseye"></i> Dashboard</a></li>
        <li><a href="portfolio.html"><i class="fa fa-tasks"></i> Query</a></li>
        <li><a href="blog.html"><i class="fa fa-globe"></i> Blog</a></li>
        <%--<li><a href="signup.html"><i class="fa fa-list-ol"></i> SignUp</a></li>--%>
        <%--<li><a href="register.html"><i class="fa fa-font"></i> Register</a></li>--%>
        <%--<li><a href="timeline.html"><i class="fa fa-font"></i> Timeline</a></li>--%>
        <%--<li><a href="forms.html"><i class="fa fa-list-ol"></i> Forms</a></li>--%>
        <%--<li><a href="typography.html"><i class="fa fa-font"></i> Typography</a></li>--%>
        <%--<li><a href="bootstrap-elements.html"><i class="fa fa-list-ul"></i> Bootstrap Elements</a></li>--%>
        <%--<li><a href="bootstrap-grid.html"><i class="fa fa-table"></i> Bootstrap Grid</a></li>--%>
      </ul>
      <ul class="nav navbar-nav navbar-right navbar-user">
        <li class="dropdown messages-dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown"><i class="fa fa-envelope"></i> Messages <span class="badge">2</span> <b class="caret"></b></a>
          <ul class="dropdown-menu">
            <li class="dropdown-header">2 New Messages</li>
            <li class="message-preview">
              <a href="#">
                <span class="avatar"><i class="fa fa-bell"></i></span>
                <span class="message">Security alert</span>
              </a>
            </li>
            <li class="divider"></li>
            <li class="message-preview">
              <a href="#">
                <span class="avatar"><i class="fa fa-bell"></i></span>
                <span class="message">Security alert</span>
              </a>
            </li>
            <li class="divider"></li>
            <li><a href="#">Go to Inbox <span class="badge">2</span></a></li>
          </ul>
        </li>
        <li class="dropdown user-dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown"><i class="fa fa-user"></i> Admin <b class="caret"></b></a>
          <ul class="dropdown-menu">
            <li><a href="#"><i class="fa fa-user"></i> Profile</a></li>
            <li><a href="#"><i class="fa fa-gear"></i> Settings</a></li>
            <li class="divider"></li>
            <li><a href="#"><i class="fa fa-power-off"></i> Log Out</a></li>

          </ul>
        </li>
        <li class="divider-vertical"></li>
        <li>
          <form class="navbar-search">
            <input type="text" placeholder="Search" class="form-control">
          </form>
        </li>
      </ul>
    </div>
  </nav>

  <div id="page-wrapper">
    <div class="row">
      <div class="col-lg-12">
        <h1>Waterwheel<small> -- A distributed system for high-rate data indexing and real-time querying</small></h1>
      </div>
    </div>
    <div class="row">
      <div class="col-md-12">
        <div class="panel panel-primary">
          <div class="panel-heading">
              <p class="panel-title" style="float: left;"><i class="fa fa-bar-chart-o"></i> Overall Insertion Throughput</p>
                  <form action="clientTest" >
                    <input type="submit" name="query" value="Fresh" style="border:0px;width: 60px;float: right;background-color:#2a9fd6; ">
                  </form>
          </div>
          <div class="panel-body">
            <div id="shieldui-chart1"></div>
          </div>
        </div>
      </div>
      <%--<div class="col-md-4">
        <div class="panel panel-primary">
          <div class="panel-heading">
            <h3 class="panel-title"><i class="fa fa-rss"></i> Feed</h3>
          </div>
          <div class="panel-body feed">
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-comment"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  <a href="#">John Doe</a> commented on <a href="#">What Makes Good Code Good</a>.
                </div>
                <div class="time pull-left">
                  3 h
                </div>
              </div>
            </section>
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-check"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  <a href="#">Merge request #42</a> has been approved by <a href="#">Jessica Lori</a>.
                </div>
                <div class="time pull-left">
                  10 h
                </div>
              </div>
            </section>
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-plus-square-o"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  New user <a href="#">Greg Wilson</a> registered.
                </div>
                <div class="time pull-left">
                  Today
                </div>
              </div>
            </section>
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-bolt"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  Server fail level raises above normal. <a href="#">See logs</a> for details.
                </div>
                <div class="time pull-left">
                  Yesterday
                </div>
              </div>
            </section>
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-archive"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  <a href="#">Database usage report</a> is ready.
                </div>
                <div class="time pull-left">
                  Yesterday
                </div>
              </div>
            </section>
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-shopping-cart"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  <a href="#">Order #233985</a> needs additional processing.
                </div>
                <div class="time pull-left">
                  Wednesday
                </div>
              </div>
            </section>
            <section class="feed-item">
              <div class="icon pull-left">
                <i class="fa fa-arrow-down"></i>
              </div>
              <div class="feed-item-body">
                <div class="text">
                  <a href="#">Load more...</a>
                </div>
              </div>
            </section>
          </div>
        </div>
      </div>--%>
    </div>
   <%-- <div class="row">
      <div class="col-lg-12">
        <div class="panel panel-primary">
          <div class="panel-heading">
            <h3 class="panel-title"><i class="fa fa-bar-chart-o"></i> Traffic Sources One month tracking </h3>
          </div>
          <div class="panel-body">
            <div id="shieldui-grid1"></div>
          </div>
        </div>
      </div>
    </div>--%>
    <div class="row">
      <%--<div class="col-lg-4">
        <div class="panel panel-primary">
          <div class="panel-heading">
            <h3 class="panel-title"><i class="fa fa-bar-chart-o"></i> Logins per week</h3>
          </div>
          <div class="panel-body">
            <div id="shieldui-chart2"></div>
          </div>
        </div>
      </div>--%>
      <div class="col-lg-12">
        <div class="panel panel-primary">
          <div class="panel-heading">
            <h3 class="panel-title"><i class="fa fa-magnet"></i> Server Overview</h3>
          </div>
          <div class="panel-body">
            <ul class="server-stats">
              <li>
                <div class="key pull-right">CPU</div>
                <div class="stat">
                  <div class="info">60% / 37°C / 3.3 Ghz</div>
                  <div class="progress progress-small">
                    <div style="width: 70%;" class="progress-bar progress-bar-danger"></div>
                  </div>
                </div>
              </li>
              <li>
                <div class="key pull-right">Mem</div>
                <div class="stat">
                  <div class="info">29% / 4GB (16 GB)</div>
                  <div class="progress progress-small">
                    <div style="width: 29%;" class="progress-bar"></div>
                  </div>
                </div>
              </li>
              <li>
                <div class="key pull-right">LAN</div>
                <div class="stat">
                  <div class="info">6 Mb/s <i class="fa fa-caret-down"></i>&nbsp; 3 Mb/s <i class="fa fa-caret-up"></i></div>
                  <div class="progress progress-small">
                    <div style="width: 48%;" class="progress-bar progress-bar-inverse"></div>
                  </div>
                </div>
              </li>
            </ul>
          </div>
        </div>

      </div>
      <%--<div class="col-lg-4">
        <header>
          <ul class="nav nav-tabs">
            <li class="active">
              <a data-toggle="tab" href="#stats">Users</a>
            </li>
            <li class="">
              <a data-toggle="tab" href="#report">Favorites</a>
            </li>
            <li class="">
              <a data-toggle="tab" href="#dropdown1">Commenters</a>
            </li>
          </ul>
        </header>
        <div class="body tab-content">
          <div class="tab-pane clearfix active" id="stats">
            <h5 class="tab-header"><i class="fa fa-calendar-o fa-2x"></i> Last logged-in users</h5>
            <ul class="news-list">
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Ivan Gorge</a></div>
                  <div class="position">Software Engineer</div>
                  <div class="time">Last logged-in: Mar 12, 11:11</div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Roman Novak</a></div>
                  <div class="position">Product Designer</div>
                  <div class="time">Last logged-in: Mar 12, 19:02</div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Teras Uotul</a></div>
                  <div class="position">Chief Officer</div>
                  <div class="time">Last logged-in: Jun 16, 2:34</div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Deral Ferad</a></div>
                  <div class="position">Financial Assistant</div>
                  <div class="time">Last logged-in: Jun 18, 4:20</div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Konrad Polerd</a></div>
                  <div class="position">Sales Manager</div>
                  <div class="time">Last logged-in: Jun 18, 5:13</div>
                </div>
              </li>
            </ul>
          </div>
          <div class="tab-pane" id="report">
            <h5 class="tab-header"><i class="fa fa-star fa-2x"></i> Popular contacts</h5>
            <ul class="news-list news-list-no-hover">
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Pol Johnsson</a></div>
                  <div class="options">
                    <button class="btn btn-xs btn-success">
                      <i class="fa fa-phone"></i>
                      Call
                    </button>
                    <button class="btn btn-xs btn-warning">
                      <i class="fa fa-envelope-o"></i>
                      Message
                    </button>
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Terry Garel</a></div>
                  <div class="options">
                    <button class="btn btn-xs btn-success">
                      <i class="fa fa-phone"></i>
                      Call
                    </button>
                    <button class="btn btn-xs btn-warning">
                      <i class="fa fa-envelope-o"></i>
                      Message
                    </button>
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Eruos Forkal</a></div>
                  <div class="options">
                    <button class="btn btn-xs btn-success">
                      <i class="fa fa-phone"></i>
                      Call
                    </button>
                    <button class="btn btn-xs btn-warning">
                      <i class="fa fa-envelope-o"></i>
                      Message
                    </button>
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Remus Reier</a></div>
                  <div class="options">
                    <button class="btn btn-xs btn-success">
                      <i class="fa fa-phone"></i>
                      Call
                    </button>
                    <button class="btn btn-xs btn-warning">
                      <i class="fa fa-envelope-o"></i>
                      Message
                    </button>
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Lover Lund</a></div>
                  <div class="options">
                    <button class="btn btn-xs btn-success">
                      <i class="fa fa-phone"></i>
                      Call
                    </button>
                    <button class="btn btn-xs btn-warning">
                      <i class="fa fa-envelope-o"></i>
                      Message
                    </button>
                  </div>
                </div>
              </li>
            </ul>
          </div>
          <div class="tab-pane" id="dropdown1">
            <h5 class="tab-header"><i class="fa fa-comments fa-2x"></i> Top Commenters</h5>
            <ul class="news-list">
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Edin Garey</a></div>
                  <div class="comment">
                    Nemo enim ipsam voluptatem quia voluptas sit aspernatur
                    aut odit aut fugit,sed quia
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Firel Lund</a></div>
                  <div class="comment">
                    Duis aute irure dolor in reprehenderit in voluptate velit
                    esse cillum dolore eu fugiat.
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Jessica Desingter</a></div>
                  <div class="comment">
                    Excepteur sint occaecat cupidatat non proident, sunt in
                    culpa qui officia deserunt.
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Novel Forel</a></div>
                  <div class="comment">
                    Sed ut perspiciatis, unde omnis iste natus error sit voluptatem accusantium doloremque.
                  </div>
                </div>
              </li>
              <li>
                <i class="fa fa-user fa-4x pull-left"></i>
                <div class="news-item-info">
                  <div class="name"><a href="#">Wedol Reier</a></div>
                  <div class="comment">
                    Laudantium, totam rem aperiam eaque ipsa, quae ab illo inventore veritatis
                    et quasi.
                  </div>
                </div>
              </li>
            </ul>
          </div>
        </div>
      </div>--%>
    </div>
  </div>
</div>
<!-- /#wrapper -->
<%--<div style="height:300px;width: 100%;background-color: white">
  <p style="padding-left: 300px;">后台传值:
  <%
    if(user != null){
     %> <%=user[0]%>&nbsp;&nbsp;<%=user[1]%>&nbsp;&nbsp;<%=user[2]%>&nbsp;&nbsp;<%=user[3]%>&nbsp;&nbsp;<%=user[4]%>&nbsp;&nbsp;<%=user[5]%><%
    }
  %>
  </p>
  <form action="clientTest" >
    <lable>查询语句:</lable>
    &lt;%&ndash;<p>&ndash;%&gt;
    &lt;%&ndash;<textarea name="sql" cols="50" rows="7" style="width:1000px; height:300px;"></textarea>&ndash;%&gt;
    &lt;%&ndash;<p>&ndash;%&gt;
    <p>123</p>
    <input type="submit" name="query" value="提交" style="height:100px;width: 100px;background-color: white;margin-left:300px;">
      <input type="text" value="<%%>" type="hidden">
    &lt;%&ndash;</p>&ndash;%&gt;
  </form>
</div>--%>

<script type="text/javascript">
    jQuery(function ($) {
        <%if(user == null){%>
            var performance = [4, 17, 22, 34, 54, 67];
        <%}
        else{%>
            var performance = [<%=user[0]%>, <%=user[1]%>, <%=user[2]%>, <%=user[3]%>, <%=user[4]%>, <%=user[5]%>]
        <%}%>

        var visits = [123, 323, 443, 32],
            traffic = [
                {
                    Source: "Direct", Amount: 323, Change: 53, Percent: 23, Target: 600
                },
                {
                    Source: "Refer", Amount: 345, Change: 34, Percent: 45, Target: 567
                },
                {
                    Source: "Social", Amount: 567, Change: 67, Percent: 23, Target: 456
                },
                {
                    Source: "Search", Amount: 234, Change: 23, Percent: 56, Target: 890
                },
                {
                    Source: "Internal", Amount: 111, Change: 78, Percent: 12, Target: 345
                }];


        $("#shieldui-chart1").shieldChart({
            theme: "dark",

            primaryHeader: {
                text: "Overall Throughput"
            },
            exportOptions: {
                image: false,
                print: false
            },
            dataSeries: [{
                seriesType: "area",
                collectionAlias: "tuple / second",
                data: performance
            }]
        });

        $("#shieldui-chart2").shieldChart({
            theme: "dark",
            primaryHeader: {
                text: "Traffic Per week"
            },
            exportOptions: {
                image: false,
                print: false
            },
            dataSeries: [{
                seriesType: "pie",
                collectionAlias: "traffic",
                data: visits
            }]
        });

        $("#shieldui-grid1").shieldGrid({
            dataSource: {
                data: traffic
            },
            sorting: {
                multiple: true
            },
            rowHover: false,
            paging: false,
            columns: [
                { field: "Source", width: "170px", title: "Source" },
                { field: "Amount", title: "Amount" },
                { field: "Percent", title: "Percent", format: "{0} %" },
                { field: "Target", title: "Target" },
            ]
        });
    });
</script>
</body>
</html>
