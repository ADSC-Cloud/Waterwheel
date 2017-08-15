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
    </script>
    <style>
      .text{
        width:120px;
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
              <a href="gentelella-master/production/index.html" class="site_title"><i class="fa fa-paw"></i> <span>Gentelella Alela!</span></a>
            </div>

            <div class="clearfix"></div>

            <!-- menu profile quick info -->
            <div class="profile clearfix">
              <div class="profile_pic">
                <img src="gentelella-master/production/images/img.jpg" alt="..." class="img-circle profile_img">
              </div>
              <div class="profile_info">
                <span>Welcome,</span>
                <h2>John Doe</h2>
              </div>
            </div>
            <!-- /menu profile quick info -->

            <br />

            <!-- sidebar menu -->
            <div id="sidebar-menu" class="main_menu_side hidden-print main_menu">
              <div class="menu_section">
                <h3>General</h3>
                <ul class="nav side-menu">
                  <li><a><i class="fa fa-home"></i> Home <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/index.html">Dashboard</a></li>
                      <li><a href="gentelella-master/production/index2.html">Dashboard2</a></li>
                      <li><a href="gentelella-master/production/index3.html">Dashboard3</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-edit"></i> Forms <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/form.html">General Form</a></li>
                      <li><a href="gentelella-master/production/form_advanced.html">Advanced Components</a></li>
                      <li><a href="gentelella-master/production/form_validation.html">Form Validation</a></li>
                      <li><a href="gentelella-master/production/form_wizards.html">Form Wizard</a></li>
                      <li><a href="gentelella-master/production/form_upload.html">Form Upload</a></li>
                      <li><a href="gentelella-master/production/form_buttons.html">Form Buttons</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-desktop"></i> UI Elements <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/general_elements.html">General Elements</a></li>
                      <li><a href="gentelella-master/production/media_gallery.html">Media Gallery</a></li>
                      <li><a href="gentelella-master/production/typography.html">Typography</a></li>
                      <li><a href="gentelella-master/production/icons.html">Icons</a></li>
                      <li><a href="gentelella-master/production/glyphicons.html">Glyphicons</a></li>
                      <li><a href="gentelella-master/production/widgets.html">Widgets</a></li>
                      <li><a href="gentelella-master/production/invoice.html">Invoice</a></li>
                      <li><a href="gentelella-master/production/inbox.html">Inbox</a></li>
                      <li><a href="gentelella-master/production/calendar.html">Calendar</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-table"></i> Tables <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/tables.html">Tables</a></li>
                      <li><a href="tables_dynamic.jsp">Table Dynamic</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-bar-chart-o"></i> Data Presentation <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/chartjs.html">Chart JS</a></li>
                      <li><a href="gentelella-master/production/chartjs2.html">Chart JS2</a></li>
                      <li><a href="gentelella-master/production/morisjs.html">Moris JS</a></li>
                      <li><a href="gentelella-master/production/echarts.html">ECharts</a></li>
                      <li><a href="gentelella-master/production/other_charts.html">Other Charts</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-clone"></i>Layouts <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/fixed_sidebar.html">Fixed Sidebar</a></li>
                      <li><a href="gentelella-master/production/fixed_footer.html">Fixed Footer</a></li>
                    </ul>
                  </li>
                </ul>
              </div>
              <div class="menu_section">
                <h3>Live On</h3>
                <ul class="nav side-menu">
                  <li><a><i class="fa fa-bug"></i> Additional Pages <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/e_commerce.html">E-commerce</a></li>
                      <li><a href="gentelella-master/production/projects.html">Projects</a></li>
                      <li><a href="gentelella-master/production/project_detail.html">Project Detail</a></li>
                      <li><a href="gentelella-master/production/contacts.html">Contacts</a></li>
                      <li><a href="gentelella-master/production/profile.html">Profile</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-windows"></i> Extras <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                      <li><a href="gentelella-master/production/page_403.html">403 Error</a></li>
                      <li><a href="gentelella-master/production/page_404.html">404 Error</a></li>
                      <li><a href="gentelella-master/production/page_500.html">500 Error</a></li>
                      <li><a href="gentelella-master/production/plain_page.html">Plain Page</a></li>
                      <li><a href="gentelella-master/production/login.html">Login Page</a></li>
                      <li><a href="gentelella-master/production/pricing_tables.html">Pricing Tables</a></li>
                    </ul>
                  </li>
                  <li><a><i class="fa fa-sitemap"></i> Multilevel Menu <span class="fa fa-chevron-down"></span></a>
                    <ul class="nav child_menu">
                        <li><a href="#level1_1">Level One</a>
                        <li><a>Level One<span class="fa fa-chevron-down"></span></a>
                          <ul class="nav child_menu">
                            <li class="sub_menu"><a href="gentelella-master/production/level2.html">Level Two</a>
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
              <a data-toggle="tooltip" data-placement="top" title="Logout" href="gentelella-master/production/login.html">
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
                    <img src="gentelella-master/production/images/img.jpg" alt="">John Doe
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
                    <li><a href="gentelella-master/production/login.html"><i class="fa fa-sign-out pull-right"></i> Log Out</a></li>
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
                        <span class="image"><img src="gentelella-master/production/images/img.jpg" alt="Profile Image" /></span>
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
                        <span class="image"><img src="gentelella-master/production/images/img.jpg" alt="Profile Image" /></span>
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
                        <span class="image"><img src="gentelella-master/production/images/img.jpg" alt="Profile Image" /></span>
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
                        <span class="image"><img src="gentelella-master/production/images/img.jpg" alt="Profile Image" /></span>
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
          <div class="">
            <div class="page-title">
              <div class="title_left">
                <h3>Users <small>Some examples to get you started</small></h3>
              </div>

              <div class="input-group">
                <div>
                  <div>
                    <input type="text" class="text" value="0" name="xLow" id="xLow" placeholder="xLow">
                    <input type="text" class="text" value="0" name="xHigh" id="xHigh" placeholder="xHigh">
                    <!-- Single button -->
                      <select class="text" id="time" name="time" style="color: #1f6377">
                        <option value="0"></option>
                        <option value="5">5s</option>
                        <option value="10">10s</option>
                        <option value="15">15s</option>
                      </select>
                    <span>
                      <input class="btn btn-info" type="submit" value="Go!" />
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
                    <h2>Default Example <small>Users</small></h2>
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