<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<html lang="zh">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <!-- Meta, title, CSS, favicons, etc. -->
  <%--<meta charset="utf-8">--%>
  <%--<meta http-equiv="X-UA-Compatible" content="IE=edge">--%>
  <%--<meta name="viewport" content="width=device-width, initial-scale=1">--%>

  <title>DataTables | Gentelella</title>

  <!-- Bootstrap -->
  <link href="gentelella-master/vendors/bootstrap/dist/css/bootstrap.min.css" rel="stylesheet">
  <!-- Font Awesome -->
  <link href="gentelella-master/vendors/font-awesome/css/font-awesome.min.css" rel="stylesheet">
  <!-- NProgress -->
  <link href="gentelella-master/vendors/nprogress/nprogress.css" rel="stylesheet">
  <!-- iCheck -->
  <link href="gentelella-master/vendors/iCheck/skins/flat/green.css" rel="stylesheet">
  <!-- Datatables -->
  <link href="gentelella-master/vendors/datatables.net-bs/css/dataTables.bootstrap.min.css" rel="stylesheet">
  <link href="gentelella-master/vendors/datatables.net-buttons-bs/css/buttons.bootstrap.min.css" rel="stylesheet">
  <link href="gentelella-master/vendors/datatables.net-fixedheader-bs/css/fixedHeader.bootstrap.min.css" rel="stylesheet">
  <link href="gentelella-master/vendors/datatables.net-responsive-bs/css/responsive.bootstrap.min.css" rel="stylesheet">
  <link href="gentelella-master/vendors/datatables.net-scroller-bs/css/scroller.bootstrap.min.css" rel="stylesheet">
  <!-- Custom Theme Style -->
  <link href="gentelella-master/build/css/custom.css" rel="stylesheet">
  <script src="js/jquery-1.10.2.min.js"/>
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
      //
      //        function checkForm() {
      //            var x = Number(document.getElementById("xLow").value);
      //            var y = Number(document.getElementById("xHigh").value);
      //            var time = Number(document.getElementById("time").value);
      //            alert(x + " " + y + " " + time);
      //        }
      //        if ((x && y && time && (x == 0 && y == 0 && time == 0) == false)) {
      //            alert("just Full or null");
      //            return false;
      //        } else {
      //            alert(x + " " + y + " " + time);
      //            return true;
      //        }
      $(document).ready(function(){

          $(".editableBox").change(function(){
              $(".timeTextBox").val($(".editableBox option:selected").html());
          });
      });

  </script>
  <style>

    .dropdown {
      position: relative;
      width: 90px;
    }
    .dropdown select
    {
      width: 100%;
    }
    .dropdown > * {
      box-sizing: border-box;
      height: 35px;
    }
    .dropdown select {
    }
    .dropdown input {
      position: absolute;
      width: calc(100% - 20px);
    }
    .text{
      width:90px;
      height:35px;
      padding:5px 9px;
      line-height:24px;
      font-size:14px;
      font-weight:bold;
      color:#333;
      border:1px solid;
      border-color:#CECECF;
      border-radius:0;
      background:white;
      box-shadow:inset 1px 1px 2px rgba(0, 0, 0, 0.1);
      -webkit-appearance:none;
    }
    .text:focus{
      outline:none;
      border-color:#92AFED;
      box-shadow:0 0 5px #92AFEC,inset 1px 1px 2px rgba(0, 0, 0, 0.1);
    }
    /*td{
      min-width:10px;
      max-width:50px;
      overflow:hidden;
      white-space:nowrap;
      text-overflow:ellipsis;
    }*/
  </style>
</head>

<body class="nav-md">
<form action="main" method="post" onsubmit="checkForm()">
  <div class="container body">
    <div class="main_container">
      <div class="col-md-3 left_col">
        <div class="left_col scroll-view">
          <div class="navbar nav_title" style="border: 0;">
            <a href="index.jsp" class="site_title" style="padding-left: 50px;"> <span>Waterwheel</span></a>
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
      <div class="right_col" role="main">
        <div class="">
          <div class="page-title">
            <div class="title_left" style="width:30%">
              <h3>Query Parameters</h3>
            </div>

            <div class="input-group">
              <div>
                <div>
                  <small style="float: left; padding: 10px">Latitude :</small>
                  <div class="dropdown" style="float: left">
                    <input type="text" class="text" value="0" name="xLow" id="xLow" placeholder="xLow">
                    <select  onchange="this.previousElementSibling.value=this.value; this.previousElementSibling.focus()">
                      <option>50</option>
                      <option>150</option>
                      <option>250</option>
                      <option>350</option>
                    </select>
                  </div>
                  <small style="float: left; padding: 10px"> to </small>
                  <div class="dropdown" style="float: left">
                    <input type="text" class="text" value="0" name="xHigh" id="xHigh" placeholder="xHigh">
                    <select  onchange="this.previousElementSibling.value=this.value; this.previousElementSibling.focus()">
                      <option>250</option>
                      <option>350</option>
                      <option>450</option>
                      <option>550</option>
                    </select>
                  </div>
                  <small style="float: left; padding: 10px">Longtitude :</small>
                  <div class="dropdown" style="float: left">
                    <input type="text" class="text" value="0" name="yLow" id="yLow" placeholder="yLow">
                    <select  onchange="this.previousElementSibling.value=this.value; this.previousElementSibling.focus()">
                      <option>50</option>
                      <option>150</option>
                      <option>250</option>
                      <option>350</option>
                    </select>
                  </div>
                  <small style="float: left; padding: 10px"> to </small>
                  <div class="dropdown" style="float: left">
                    <input type="text" class="text" value="0" name="yHigh" id="yHigh" placeholder="yHigh">
                    <select  onchange="this.previousElementSibling.value=this.value; this.previousElementSibling.focus()">
                      <option>250</option>
                      <option>350</option>
                      <option>450</option>
                      <option>550</option>
                    </select>
                  </div>
                  <!-- Single button -->
                  <small style="padding: 10px; float: left">Time :</small>
                  <select class="text" id="time" name="time" style="color: #1f6377; float: left; margin-right: 10px">
                    <option value="5">5s</option>
                    <option value="10">10s</option>
                    <option value="15">15s</option>
                  </select>
                  <span>
                      <input style="float: left" class="btn btn-info" type="submit" value="Query!" />
                    </span>
                </div>
              </div>
            </div>
          </div>

          <div class="clearfix"></div>

          <div class="row">
            <div class="col-md-12 col-sm-12 col-xs-12">
              <div class="x_panel">
                <div class="x_title">
                  <h2>Query Rusult</h2>
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
                    Query Response Time :  <code>${queryTime }ms</code>
                  </p>
                  <table id="datatable" class="table table-striped table-bordered">
                    <thead>
                    <tr>

                      <c:forEach var="fieldName" items="${fieldNames}">
                        <th>${fieldName}</th>
                      </c:forEach>
                    </tr>
                    </thead>


                    <tbody>

                    <c:forEach var="diary" items="${diaryList }" begin="0" end="1024">
                      <tr>
                        <c:forEach items="${diary}" var="msg">
                          <td>${msg }</td>
                        </c:forEach>
                      </tr>
                    </c:forEach>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <!-- /page content -->
      <!-- /footer content -->
    </div>
  </div>

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
  <!-- Datatables -->
  <script src="gentelella-master/vendors/datatables.net/js/jquery.dataTables.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-bs/js/dataTables.bootstrap.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-buttons/js/dataTables.buttons.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-buttons-bs/js/buttons.bootstrap.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-buttons/js/buttons.flash.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-buttons/js/buttons.html5.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-buttons/js/buttons.print.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-fixedheader/js/dataTables.fixedHeader.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-keytable/js/dataTables.keyTable.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-responsive/js/dataTables.responsive.min.js"></script>
  <script src="gentelella-master/vendors/datatables.net-responsive-bs/js/responsive.bootstrap.js"></script>
  <script src="gentelella-master/vendors/datatables.net-scroller/js/dataTables.scroller.min.js"></script>
  <script src="gentelella-master/vendors/jszip/dist/jszip.min.js"></script>
  <script src="gentelella-master/vendors/pdfmake/build/pdfmake.min.js"></script>
  <script src="gentelella-master/vendors/pdfmake/build/vfs_fonts.js"></script>

  <!-- Custom Theme Scripts -->
  <script src="gentelella-master/build/js/custom.min.js"></script>
</form>
</body>
</html>